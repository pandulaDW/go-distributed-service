package discovery

import (
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
	"net"
)

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// Handler represents some component in this service that needs to know
// when a server joins or leaves the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// Membership is the type wrapping Serf to provide discovery and cluster
// membership to the service.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// New creates a Membership with the required configuration and the event handler
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// setupSerf() creates and configures a Serf instance and starts the eventsHandler()
// goroutine to handle Serf’s events.
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName

	// create serf instance
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	// event handler to run custom logic when an event fires
	go m.eventHandler()

	// join cluster, if not the initial member
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// eventHandler runs in a loop reading events sent by Serf into the events channel, handling
// each incoming event according to the event’s type. When a node joins or leaves the cluster,
// Serf sends an event to all nodes, including the node that joined or left the cluster
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal returns whether the given Serf member is the local member by
// checking the members’ names
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Leave tells this member to leave the Serf cluster
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// Members returns a point-in-time snapshot of the cluster’s Serf members.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// LogError logs the given error and message
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg, zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
