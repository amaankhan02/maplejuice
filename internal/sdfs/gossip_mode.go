package sdfs

type GossipModeValue int

const (
	GOSSIP_NORMAL    GossipModeValue = 0
	GOSSIP_SUSPICION GossipModeValue = 1
)

func (gm GossipModeValue) String() string {
	if gm == GOSSIP_SUSPICION {
		return "Suspicion"
	} else {
		return "Normal"
	}
}

type GossipMode struct {
	Mode          GossipModeValue
	VersionNumber int
}
