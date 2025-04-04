// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package p2p

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/pingcap/tiflow/proto/p2p"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	gRPCPeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	messageServerReportsIndividualMessageSize = true
)

// MessageServerConfig stores configurations for the MessageServer
type MessageServerConfig struct {
	// The maximum number of entries to be cached for topics with no handler registered
	MaxPendingMessageCountPerTopic int
	// The maximum number of unhandled internal tasks for the main thread.
	MaxPendingTaskCount int
	// The size of the channel for pending messages before sending them to gRPC.
	SendChannelSize int
	// The interval between ACKs.
	AckInterval time.Duration
	// The size of the goroutine pool for running the handlers.
	WorkerPoolSize int
	// The maximum send rate per stream (per peer).
	SendRateLimitPerStream float64
	// The maximum number of peers acceptable by this server
	MaxPeerCount int
	// Semver of the server. Empty string means no version check.
	ServerVersion string
	// MaxRecvMsgSize is the maximum message size in bytes TiCDC can receive.
	MaxRecvMsgSize int

	// After a duration of this time if the server doesn't see any activity it
	// pings the client to see if the transport is still alive.
	KeepAliveTime time.Duration

	// After having pinged for keepalive check, the server waits for a duration
	// of Timeout and if no activity is seen even after that the connection is
	// closed.
	KeepAliveTimeout time.Duration

	// The maximum time duration to wait before forcefully removing a handler.
	//
	// waitUnregisterHandleTimeout specifies how long to wait for
	// the topic handler to consume all pending messages before
	// forcefully unregister the handler.
	// For a correct implementation of the handler, the time it needs
	// to consume these messages is minimal, as the handler is not
	// expected to block on channels, etc.
	WaitUnregisterHandleTimeoutThreshold time.Duration
}

// cdcPeer is used to store information on one connected client.
type cdcPeer struct {
	PeerID string // unique ID of the client

	// Epoch is increased when the client retries.
	// It is used to avoid two streams from the same client racing with each other.
	// This can happen because the MessageServer might not immediately know that
	// a stream has become stale.
	Epoch int64

	// necessary information on the stream.
	sender *streamHandle

	// valid says whether the peer is valid.
	// Note that it does not need to be thread-safe
	// because it should only be accessed in MessageServer.run().
	valid bool

	metricsAckCount prometheus.Counter
}

func newCDCPeer(senderID NodeID, epoch int64, sender *streamHandle) *cdcPeer {
	return &cdcPeer{
		PeerID: senderID,
		Epoch:  epoch,
		sender: sender,
		valid:  true,
		metricsAckCount: serverAckCount.With(prometheus.Labels{
			"to": senderID,
		}),
	}
}

func (p *cdcPeer) abort(ctx context.Context, err error) {
	if !p.valid {
		log.Panic("p2p: aborting invalid peer", zap.String("peer", p.PeerID))
	}

	defer func() {
		p.valid = false
	}()
	if sendErr := p.sender.Send(ctx, errorToRPCResponse(err)); sendErr != nil {
		log.Warn("could not send error to peer", zap.Error(err),
			zap.NamedError("sendErr", sendErr))
		return
	}
	log.Debug("sent error to peer", zap.Error(err))
}

// MessageServer is an implementation of the gRPC server for the peer-to-peer system
type MessageServer struct {
	serverID NodeID

	// Each topic has at most one registered event handle,
	// registered with a WorkerPool.
	handlers map[Topic]workerpool.EventHandle

	peerLock sync.RWMutex
	peers    map[string]*cdcPeer // all currently connected clients

	// pendingMessages store messages for topics with NO registered handle.
	// This can happen when the server is slow.
	// The upper limit of pending messages is restricted by
	// MaxPendingMessageCountPerTopic in MessageServerConfig.
	pendingMessages map[topicSenderPair][]pendingMessageEntry

	acks *ackManager

	// taskQueue is used to store internal tasks MessageServer
	// needs to execute serially.
	taskQueue chan interface{}

	// The WorkerPool instance used to execute message handlers.
	pool workerpool.WorkerPool

	isRunning int32 // atomic
	closeCh   chan struct{}

	config *MessageServerConfig // read only
}

type taskOnMessageBatch struct {
	// for grpc msgs
	streamMeta     *p2p.StreamMeta
	messageEntries []*p2p.MessageEntry

	// for internal msgs
	rawMessageEntries []RawMessageEntry
}

type taskOnRegisterPeer struct {
	sender     *streamHandle
	clientAddr string // for logging
}

type taskOnDeregisterPeer struct {
	peerID string
}

type taskOnRegisterHandler struct {
	topic   string
	handler workerpool.EventHandle
	done    chan struct{}
}

type taskOnDeregisterHandler struct {
	topic string
	done  chan struct{}
}

// taskDebugDelay is used in unit tests to artificially block the main
// goroutine of the server. It is not used in other places.
type taskDebugDelay struct {
	doneCh chan struct{}
}

// NewMessageServer creates a new MessageServer
func NewMessageServer(serverID NodeID, config *MessageServerConfig) *MessageServer {
	return &MessageServer{
		serverID:        serverID,
		handlers:        make(map[string]workerpool.EventHandle),
		peers:           make(map[string]*cdcPeer),
		pendingMessages: make(map[topicSenderPair][]pendingMessageEntry),
		acks:            newAckManager(),
		taskQueue:       make(chan interface{}, config.MaxPendingTaskCount),
		pool:            workerpool.NewDefaultWorkerPool(config.WorkerPoolSize),
		closeCh:         make(chan struct{}),
		config:          config,
	}
}

// Run starts the MessageServer's worker goroutines.
// It must be running to provide the gRPC service.
func (m *MessageServer) Run(ctx context.Context, localCh <-chan RawMessageEntry) error {
	atomic.StoreInt32(&m.isRunning, 1)
	defer func() {
		atomic.StoreInt32(&m.isRunning, 0)
		close(m.closeCh)
	}()

	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return errors.Trace(m.run(ctx))
	})

	errg.Go(func() error {
		return errors.Trace(m.pool.Run(ctx))
	})

	if localCh != nil {
		errg.Go(func() error {
			return errors.Trace(m.receiveLocalMessage(ctx, localCh))
		})
	}

	return errg.Wait()
}

func (m *MessageServer) run(ctx context.Context) error {
	ticker := time.NewTicker(m.config.AckInterval)
	defer ticker.Stop()

	for {
		failpoint.Inject("ServerInjectTaskDelay", func() {
			log.Info("channel size", zap.Int("len", len(m.taskQueue)))
		})
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			m.tick(ctx)
		case task := <-m.taskQueue:
			switch task := task.(type) {
			case taskOnMessageBatch:
				for _, entry := range task.rawMessageEntries {
					m.handleRawMessage(ctx, entry)
				}
				for _, entry := range task.messageEntries {
					m.handleMessage(ctx, task.streamMeta, entry)
				}
			case taskOnRegisterHandler:
				// FIXME better error handling here.
				// Notes: registering a handler is not expected to fail unless a context is cancelled.
				// The current error handling here will cause the server to exit, which is not ideal,
				// but will not cause service to be interrupted because the `ctx` involved here will not
				// be cancelled unless the server is exiting.
				m.registerHandler(ctx, task.topic, task.handler, task.done)
				log.Debug("handler registered", zap.String("topic", task.topic))
			case taskOnDeregisterHandler:
				if handler, ok := m.handlers[task.topic]; ok {
					delete(m.handlers, task.topic)
					go func() {
						err := handler.GracefulUnregister(ctx, m.config.WaitUnregisterHandleTimeoutThreshold)
						if err != nil {
							// This can only happen if `ctx` is cancelled or the workerpool
							// fails to unregister the handle in time, which can be caused
							// by inappropriate blocking inside the handler.
							// We use `DPanic` here so that any unexpected blocking can be
							// caught in tests, but in the same time we can provide better
							// resilience in production (`DPanic` does not panic in production).
							//
							// Note: Even if `GracefulUnregister` does fail, the handle is still
							// unregistered, only forcefully.
							log.Warn("failed to gracefully unregister handle",
								zap.Error(err))
						}
						log.Debug("handler deregistered", zap.String("topic", task.topic))
						if task.done != nil {
							close(task.done)
						}
					}()
				} else {
					// This is to make deregistering a handler idempotent.
					// Idempotency here will simplify error handling for the callers of this package.
					log.Warn("handler not found", zap.String("topic", task.topic))
					if task.done != nil {
						close(task.done)
					}
				}
			case taskOnRegisterPeer:
				log.Debug("taskOnRegisterPeer",
					zap.String("sender", task.sender.GetStreamMeta().SenderId),
					zap.Int64("epoch", task.sender.GetStreamMeta().Epoch))
				if err := m.registerPeer(ctx, task.sender, task.clientAddr); err != nil {
					if cerror.ErrPeerMessageStaleConnection.Equal(err) || cerror.ErrPeerMessageDuplicateConnection.Equal(err) {
						// These two errors should not affect other peers
						if err1 := task.sender.Send(ctx, errorToRPCResponse(err)); err1 != nil {
							return errors.Trace(err)
						}
						continue // to handling the next task
					}
					return errors.Trace(err)
				}
			case taskOnDeregisterPeer:
				log.Info("taskOnDeregisterPeer", zap.String("peerID", task.peerID))
				m.deregisterPeerByID(ctx, task.peerID)
			case taskDebugDelay:
				log.Info("taskDebugDelay started")
				select {
				case <-ctx.Done():
					log.Info("taskDebugDelay canceled")
					return errors.Trace(ctx.Err())
				case <-task.doneCh:
				}
				log.Info("taskDebugDelay ended")
			}
		}
	}
}

func (m *MessageServer) tick(ctx context.Context) {
	var peersToDeregister []*cdcPeer
	defer func() {
		for _, peer := range peersToDeregister {
			// err is nil because the peers are gone already, so sending errors will not succeed.
			m.deregisterPeer(ctx, peer, nil)
		}
	}()

	m.peerLock.RLock()
	defer m.peerLock.RUnlock()

	for _, peer := range m.peers {
		var acks []*p2p.Ack
		m.acks.Range(peer.PeerID, func(topic Topic, seq Seq) bool {
			acks = append(acks, &p2p.Ack{
				Topic:   topic,
				LastSeq: seq,
			})
			return true
		})

		if len(acks) == 0 {
			continue
		}

		peer.metricsAckCount.Inc()
		err := peer.sender.Send(ctx, p2p.SendMessageResponse{
			Ack:        acks,
			ExitReason: p2p.ExitReason_OK, // ExitReason_Ok means not exiting
		})
		if err != nil {
			log.Warn("sending response to peer failed", zap.Error(err))
			if cerror.ErrPeerMessageInternalSenderClosed.Equal(err) {
				peersToDeregister = append(peersToDeregister, peer)
			}
		}
	}
}

func (m *MessageServer) deregisterPeer(ctx context.Context, peer *cdcPeer, err error) {
	log.Info("Deregistering peer",
		zap.String("sender", peer.PeerID),
		zap.Int64("epoch", peer.Epoch),
		zap.Error(err))

	m.peerLock.Lock()
	// TODO add a tombstone state to facilitate GC'ing the acks records associated with the peer.
	delete(m.peers, peer.PeerID)
	m.peerLock.Unlock()
	if err != nil {
		peer.abort(ctx, err)
	}
}

func (m *MessageServer) deregisterPeerByID(ctx context.Context, peerID string) {
	m.peerLock.Lock()
	peer, ok := m.peers[peerID]
	m.peerLock.Unlock()
	if !ok {
		log.Warn("peer not found", zap.String("peerID", peerID))
		return
	}
	m.deregisterPeer(ctx, peer, nil)
}

// ScheduleDeregisterPeerTask schedules a task to deregister a peer.
func (m *MessageServer) ScheduleDeregisterPeerTask(ctx context.Context, peerID string) error {
	return m.scheduleTask(ctx, taskOnDeregisterPeer{peerID: peerID})
}

// We use an empty interface to hold the information on the type of the object
// that we want to deserialize a message to.
// We pass an object of the desired type, and use `reflect.TypeOf` to extract the type,
// and then when we need it, we can use `reflect.New` to allocate a new object of this
// type.
type typeInformation = interface{}

// SyncAddHandler registers a handler for messages in a given topic and waits for the operation
// to complete.
func (m *MessageServer) SyncAddHandler(
	ctx context.Context,
	topic string,
	tpi typeInformation,
	fn func(string, interface{}) error,
) (<-chan error, error) {
	doneCh, errCh, err := m.AddHandler(ctx, topic, tpi, fn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case <-doneCh:
	case <-m.closeCh:
		return nil, cerror.ErrPeerMessageServerClosed.GenWithStackByArgs()
	}
	return errCh, nil
}

// AddHandler registers a handler for messages in a given topic.
func (m *MessageServer) AddHandler(
	ctx context.Context,
	topic string,
	tpi typeInformation,
	fn func(string, interface{}) error,
) (chan struct{}, <-chan error, error) {
	tp := reflect.TypeOf(tpi)

	metricsServerRepeatedMessageCount := serverRepeatedMessageCount.MustCurryWith(prometheus.Labels{
		"topic": topic,
	})

	poolHandle := m.pool.RegisterEvent(func(ctx context.Context, argsI interface{}) error {
		args, ok := argsI.(poolEventArgs)
		if !ok {
			// Handle message from local.
			if err := fn(m.serverID, argsI); err != nil {
				return errors.Trace(err)
			}
			return nil
		}
		sm := args.streamMeta
		entry := args.entry
		e := reflect.New(tp.Elem()).Interface()

		lastAck := m.acks.Get(sm.SenderId, entry.GetTopic())
		if lastAck >= entry.Sequence {
			metricsServerRepeatedMessageCount.With(prometheus.Labels{
				"from": sm.SenderAdvertisedAddr,
			}).Inc()

			log.Debug("skipping peer message",
				zap.String("senderID", sm.SenderId),
				zap.String("topic", topic),
				zap.Int64("skippedSeq", entry.Sequence),
				zap.Int64("lastAck", lastAck))
			return nil
		}
		if lastAck != initAck && entry.Sequence > lastAck+1 {
			// We detected a message loss at seq = (lastAck+1).
			// Note that entry.Sequence == lastAck+1 is actual a requirement
			// on the continuity of sequence numbers, which can be guaranteed
			// by the client locally.

			// A data loss can only happen if the receiver's handler had failed to
			// unregister before the receiver restarted. This is expected to be
			// rare and indicates problems with the receiver's handler.
			// It is expected to happen only with extreme system latency or buggy code.
			//
			// Reports an error so that the receiver can gracefully exit.
			return cerror.ErrPeerMessageDataLost.GenWithStackByArgs(entry.Topic, lastAck+1)
		}

		if err := unmarshalMessage(entry.Content, e); err != nil {
			return cerror.WrapError(cerror.ErrPeerMessageDecodeError, err)
		}

		if err := fn(sm.SenderId, e); err != nil {
			return errors.Trace(err)
		}

		m.acks.Set(sm.SenderId, entry.GetTopic(), entry.GetSequence())

		return nil
	}).OnExit(func(err error) {
		log.Warn("topic handler returned error", zap.Error(err))
		_ = m.scheduleTask(ctx, taskOnDeregisterHandler{
			topic: topic,
		})
	})

	doneCh := make(chan struct{})

	if err := m.scheduleTask(ctx, taskOnRegisterHandler{
		topic:   topic,
		handler: poolHandle,
		done:    doneCh,
	}); err != nil {
		return nil, nil, errors.Trace(err)
	}

	return doneCh, poolHandle.ErrCh(), nil
}

// SyncRemoveHandler removes the registered handler for the given topic and wait
// for the operation to complete.
func (m *MessageServer) SyncRemoveHandler(ctx context.Context, topic string) error {
	doneCh, err := m.RemoveHandler(ctx, topic)
	if err != nil {
		return errors.Trace(err)
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-doneCh:
	case <-m.closeCh:
		log.Debug("message server is closed while a handler is being removed",
			zap.String("topic", topic))
		return nil
	}

	return nil
}

// RemoveHandler removes the registered handler for the given topic.
func (m *MessageServer) RemoveHandler(ctx context.Context, topic string) (chan struct{}, error) {
	doneCh := make(chan struct{})
	if err := m.scheduleTask(ctx, taskOnDeregisterHandler{
		topic: topic,
		done:  doneCh,
	}); err != nil {
		return nil, errors.Trace(err)
	}

	return doneCh, nil
}

func (m *MessageServer) registerHandler(ctx context.Context, topic string, handler workerpool.EventHandle, doneCh chan struct{}) {
	defer close(doneCh)

	if _, ok := m.handlers[topic]; ok {
		// allow replacing the handler here would result in behaviors difficult to define.
		// Continuing the program when there is a risk of duplicate handlers will likely
		// result in undefined behaviors, so we panic here.
		log.Panic("duplicate handlers",
			zap.String("topic", topic))
	}

	m.handlers[topic] = handler
	m.handlePendingMessages(ctx, topic)
}

// handlePendingMessages must be called with `handlerLock` taken exclusively.
func (m *MessageServer) handlePendingMessages(ctx context.Context, topic string) {
	for key, entries := range m.pendingMessages {
		if key.Topic != topic {
			continue
		}

		for _, entry := range entries {
			if entry.StreamMeta != nil {
				m.handleMessage(ctx, entry.StreamMeta, entry.Entry)
			} else {
				m.handleRawMessage(ctx, entry.RawEntry)
			}
		}

		delete(m.pendingMessages, key)
	}
}

func (m *MessageServer) registerPeer(
	ctx context.Context,
	sender *streamHandle,
	clientIP string,
) error {
	streamMeta := sender.GetStreamMeta()

	log.Info("peer connection received",
		zap.String("senderID", streamMeta.SenderId),
		zap.String("senderAdvertiseAddr", streamMeta.SenderAdvertisedAddr),
		zap.String("addr", clientIP),
		zap.Int64("epoch", streamMeta.Epoch))

	m.peerLock.Lock()
	peer, ok := m.peers[streamMeta.SenderId]
	if !ok {
		peerCount := len(m.peers)
		if peerCount > m.config.MaxPeerCount {
			m.peerLock.Unlock()
			return cerror.ErrPeerMessageToManyPeers.GenWithStackByArgs(peerCount)
		}
		// no existing peer
		m.peers[streamMeta.SenderId] = newCDCPeer(streamMeta.SenderId, streamMeta.Epoch, sender)
		m.peerLock.Unlock()
	} else {
		m.peerLock.Unlock()
		// there is an existing peer
		if peer.Epoch > streamMeta.Epoch {
			log.Warn("incoming connection is stale",
				zap.String("senderID", streamMeta.SenderId),
				zap.String("addr", clientIP),
				zap.Int64("epoch", streamMeta.Epoch))

			// the current stream is stale
			return cerror.ErrPeerMessageStaleConnection.GenWithStackByArgs(streamMeta.Epoch /* old */, peer.Epoch /* new */)
		} else if peer.Epoch < streamMeta.Epoch {
			err := cerror.ErrPeerMessageStaleConnection.GenWithStackByArgs(peer.Epoch /* old */, streamMeta.Epoch /* new */)
			m.deregisterPeer(ctx, peer, err)
			m.peerLock.Lock()
			m.peers[streamMeta.SenderId] = newCDCPeer(streamMeta.SenderId, streamMeta.Epoch, sender)
			m.peerLock.Unlock()
		} else {
			log.Warn("incoming connection is duplicate",
				zap.String("senderID", streamMeta.SenderId),
				zap.String("addr", clientIP),
				zap.Int64("epoch", streamMeta.Epoch))

			return cerror.ErrPeerMessageDuplicateConnection.GenWithStackByArgs(streamMeta.Epoch)
		}
	}

	return nil
}

func (m *MessageServer) scheduleTask(ctx context.Context, task interface{}) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case m.taskQueue <- task:
	default:
		return cerror.ErrPeerMessageTaskQueueCongested.GenWithStackByArgs()
	}
	return nil
}

func (m *MessageServer) scheduleTaskBlocking(ctx context.Context, task interface{}) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case m.taskQueue <- task:
	}
	return nil
}

func (m *MessageServer) receiveLocalMessage(ctx context.Context, localCh <-chan RawMessageEntry) error {
	batchRawMessages := []RawMessageEntry{}
	sendTaskBlocking := func() {
		if len(batchRawMessages) == 0 {
			return
		}
		_ = m.scheduleTaskBlocking(ctx, taskOnMessageBatch{
			rawMessageEntries: batchRawMessages,
		})
		batchRawMessages = []RawMessageEntry{}
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case entry, ok := <-localCh:
			if !ok {
				errMsg := "local server stream closed since the channel is closed"
				return cerror.ErrPeerMessageServerClosed.GenWithStackByArgs(errMsg)
			}
			batchRawMessages = append(batchRawMessages, entry)

			if len(batchRawMessages) >= 1024 {
				sendTaskBlocking()
			}
		case <-ticker.C:
			sendTaskBlocking()
		}
	}
}

// SendMessage implements the gRPC call SendMessage.
func (m *MessageServer) SendMessage(stream p2p.CDCPeerToPeer_SendMessageServer) error {
	ctx := stream.Context()
	packet, err := stream.Recv()
	if err != nil {
		return errors.Trace(err)
	}

	if err := m.verifyStreamMeta(packet.Meta); err != nil {
		msg := errorToRPCResponse(err)
		_ = stream.Send(&msg)
		return errors.Trace(err)
	}

	metricsServerStreamCount := serverStreamCount.With(prometheus.Labels{
		"from": packet.Meta.SenderAdvertisedAddr,
	})
	metricsServerStreamCount.Add(1)
	defer metricsServerStreamCount.Sub(1)

	sendCh := make(chan p2p.SendMessageResponse, m.config.SendChannelSize)
	streamHandle := newStreamHandle(packet.Meta, sendCh)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errg, egCtx := errgroup.WithContext(ctx)

	// receive messages from the sender
	errg.Go(func() error {
		defer streamHandle.Close()
		clientSocketAddr := unknownPeerLabel
		if p, ok := gRPCPeer.FromContext(egCtx); ok {
			clientSocketAddr = p.Addr.String()
		}
		if err := m.receive(egCtx, clientSocketAddr, stream, streamHandle); err != nil {
			log.Warn("peer-to-peer message handler error", zap.Error(err))
			select {
			case <-egCtx.Done():
				log.Warn("error receiving from peer", zap.Error(egCtx.Err()))
				return errors.Trace(egCtx.Err())
			case sendCh <- errorToRPCResponse(err):
			default:
				log.Warn("sendCh congested, could not send error", zap.Error(err))
				return errors.Trace(err)
			}
		}
		return nil
	})

	// send acks to the sender
	errg.Go(func() error {
		rl := rate.NewLimiter(rate.Limit(m.config.SendRateLimitPerStream), 1)
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case resp, ok := <-sendCh:
				if !ok {
					log.Info("peer stream handle is closed",
						zap.String("peerAddr", streamHandle.GetStreamMeta().SenderAdvertisedAddr),
						zap.String("peerID", streamHandle.GetStreamMeta().SenderId))
					// cancel the stream when sendCh is closed
					cancel()
					return nil
				}
				if err := rl.Wait(ctx); err != nil {
					return errors.Trace(err)
				}
				if err := stream.Send(&resp); err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	// We need to select on `m.closeCh` and `ctx.Done()` to make sure that
	// the request handler returns when we need it to.
	// We cannot allow `Send` and `Recv` to block the handler when it needs to exit,
	// such as when the MessageServer is exiting due to an error.
	select {
	case <-ctx.Done():
		return status.New(codes.Canceled, "context canceled").Err()
	case <-m.closeCh:
		return status.New(codes.Aborted, "message server is closing").Err()
	}

	// NB: `errg` will NOT be waited on because due to the limitation of grpc-go, we cannot cancel Send & Recv
	// with contexts, and the only reliable way to cancel these operations is to return the gRPC call handler,
	// namely this function.
}

func (m *MessageServer) receive(
	ctx context.Context,
	clientSocketAddr string,
	stream p2p.CDCPeerToPeer_SendMessageServer,
	streamHandle *streamHandle,
) error {
	// We use scheduleTaskBlocking because blocking here is acceptable.
	// Blocking here will cause grpc-go to back propagate the pressure
	// to the client, which is what we want.
	if err := m.scheduleTaskBlocking(ctx, taskOnRegisterPeer{
		sender:     streamHandle,
		clientAddr: clientSocketAddr,
	}); err != nil {
		return errors.Trace(err)
	}

	metricsServerMessageCount := serverMessageCount.With(prometheus.Labels{
		"from": streamHandle.GetStreamMeta().SenderAdvertisedAddr,
	})
	metricsServerMessageBatchHistogram := serverMessageBatchHistogram.With(prometheus.Labels{
		"from": streamHandle.GetStreamMeta().SenderAdvertisedAddr,
	})
	metricsServerMessageBatchBytesHistogram := serverMessageBatchBytesHistogram.With(prometheus.Labels{
		"from": streamHandle.GetStreamMeta().SenderAdvertisedAddr,
	})
	metricsServerMessageBytesHistogram := serverMessageBytesHistogram.With(prometheus.Labels{
		"from": streamHandle.GetStreamMeta().SenderAdvertisedAddr,
	})

	for {
		failpoint.Inject("ServerInjectServerRestart", func() {
			_ = stream.Send(&p2p.SendMessageResponse{
				ExitReason: p2p.ExitReason_CONGESTED,
			})
			failpoint.Return(errors.Trace(errors.New("injected error")))
		})

		packet, err := stream.Recv()
		if err != nil {
			return errors.Trace(err)
		}

		batchSize := len(packet.GetEntries())
		log.Debug("received packet", zap.String("streamHandle", streamHandle.GetStreamMeta().SenderId),
			zap.Int("numEntries", batchSize))

		batchBytes := packet.Size()
		metricsServerMessageBatchBytesHistogram.Observe(float64(batchBytes))
		metricsServerMessageBatchHistogram.Observe(float64(batchSize))
		metricsServerMessageCount.Add(float64(batchSize))

		entries := packet.GetEntries()
		if batchSize > 0 {
			if messageServerReportsIndividualMessageSize /* true for now */ {
				// Note that this can be costly if the number of messages is huge.
				// However, the current usage of this package in TiCDC should not
				// cause any problem, as the messages are for metadata only.
				for _, entry := range entries {
					messageWireSize := entry.Size()
					metricsServerMessageBytesHistogram.Observe(float64(messageWireSize))
				}
			}

			// See the comment above on why use scheduleTaskBlocking.
			if err := m.scheduleTaskBlocking(ctx, taskOnMessageBatch{
				streamMeta:     streamHandle.GetStreamMeta(),
				messageEntries: packet.GetEntries(),
			}); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (m *MessageServer) handleRawMessage(ctx context.Context, entry RawMessageEntry) {
	handler, ok := m.handlers[entry.topic]
	if !ok {
		// handler not found
		pendingMessageKey := topicSenderPair{
			Topic:    entry.topic,
			SenderID: m.serverID,
		}
		pendingEntries := m.pendingMessages[pendingMessageKey]
		m.pendingMessages[pendingMessageKey] = append(pendingEntries, pendingMessageEntry{
			RawEntry: entry,
		})
		if len(m.pendingMessages[pendingMessageKey]) >= m.config.MaxPendingMessageCountPerTopic {
			delete(m.pendingMessages, pendingMessageKey)
			log.Warn("Topic congested because no handler has been registered", zap.Any("topic", pendingMessageKey))
		}
		return
	}
	// handler is found
	if err := handler.AddEvent(ctx, entry.value); err != nil {
		// just ignore the message if handler returns an error
		errMsg := "Failed to process message due to a handler error"
		log.Debug(errMsg, zap.Error(err), zap.String("topic", entry.topic))
	}
}

func (m *MessageServer) handleMessage(ctx context.Context, streamMeta *p2p.StreamMeta, entry *p2p.MessageEntry) {
	m.peerLock.RLock()
	peer, ok := m.peers[streamMeta.SenderId]
	m.peerLock.RUnlock()
	if !ok || peer.Epoch != streamMeta.GetEpoch() {
		log.Debug("p2p: message without corresponding peer",
			zap.String("topic", entry.GetTopic()),
			zap.Int64("seq", entry.GetSequence()))
		return
	}

	// Drop messages from invalid peers
	if !peer.valid {
		return
	}

	topic := entry.GetTopic()
	pendingMessageKey := topicSenderPair{
		Topic:    topic,
		SenderID: streamMeta.SenderId,
	}
	handler, ok := m.handlers[topic]
	if !ok {
		// handler not found
		pendingEntries := m.pendingMessages[pendingMessageKey]
		if len(pendingEntries) > m.config.MaxPendingMessageCountPerTopic {
			log.Warn("Topic congested because no handler has been registered", zap.String("topic", topic))
			delete(m.pendingMessages, pendingMessageKey)
			m.deregisterPeer(ctx, peer, cerror.ErrPeerMessageTopicCongested.FastGenByArgs())
			return
		}
		m.pendingMessages[pendingMessageKey] = append(pendingEntries, pendingMessageEntry{
			StreamMeta: streamMeta,
			Entry:      entry,
		})

		return
	}

	// handler is found
	if err := handler.AddEvent(ctx, poolEventArgs{
		streamMeta: streamMeta,
		entry:      entry,
	}); err != nil {
		log.Warn("Failed to process message due to a handler error",
			zap.Error(err), zap.String("topic", topic))
		m.deregisterPeer(ctx, peer, err)
	}
}

func (m *MessageServer) verifyStreamMeta(streamMeta *p2p.StreamMeta) error {
	if streamMeta == nil {
		return cerror.ErrPeerMessageIllegalMeta.GenWithStackByArgs()
	}

	if streamMeta.ReceiverId != m.serverID {
		return cerror.ErrPeerMessageReceiverMismatch.GenWithStackByArgs(
			m.serverID,            // expected
			streamMeta.ReceiverId, // actual
		)
	}

	return nil
}

type topicSenderPair struct {
	Topic    string
	SenderID string
}

type pendingMessageEntry struct {
	// for grpc msgs
	StreamMeta *p2p.StreamMeta
	Entry      *p2p.MessageEntry

	// for local msgs
	RawEntry RawMessageEntry
}

func errorToRPCResponse(err error) p2p.SendMessageResponse {
	if cerror.ErrPeerMessageTopicCongested.Equal(err) ||
		cerror.ErrPeerMessageTaskQueueCongested.Equal(err) {

		return p2p.SendMessageResponse{
			ExitReason:   p2p.ExitReason_CONGESTED,
			ErrorMessage: err.Error(),
		}
	} else if cerror.ErrPeerMessageStaleConnection.Equal(err) {
		return p2p.SendMessageResponse{
			ExitReason:   p2p.ExitReason_STALE_CONNECTION,
			ErrorMessage: err.Error(),
		}
	} else if cerror.ErrPeerMessageReceiverMismatch.Equal(err) {
		return p2p.SendMessageResponse{
			ExitReason:   p2p.ExitReason_CAPTURE_ID_MISMATCH,
			ErrorMessage: err.Error(),
		}
	}
	return p2p.SendMessageResponse{
		ExitReason:   p2p.ExitReason_UNKNOWN,
		ErrorMessage: err.Error(),
	}
}

type poolEventArgs struct {
	streamMeta *p2p.StreamMeta
	entry      *p2p.MessageEntry
}
