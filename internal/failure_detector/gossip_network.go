package failure_detector

import (
	"bytes"
	"cs425_mp4/internal/core"
	"encoding/gob"
	"errors"
)

type GossipUDPPacket struct {
	MyGossipMode        GossipMode
	MyMembershipListMap map[core.NodeID]MembershipListEntry
}

/*
Given the following parameters, it serializes it into byte buffer and returns it, so you
can send the output over the UDP tcp_net
*/
func CreateGossipUDPPacket(gossipMode GossipMode, membershipList *MembershipList) ([]byte, error) {
	packet := GossipUDPPacket{gossipMode, membershipList.MemList}
	ret, err := SerializeGossipUDPPacket(&packet)
	if err != nil {
		return make([]byte, 0), errors.New("failed to serialize packet")
	}

	return ret, nil
}

func ReadGossipUDPPacket(packetData []byte) (GossipMode, *MembershipList, error) {
	if len(packetData) == 0 {
		return GossipMode{}, nil, errors.New("packetData is 0 bytes... Unable to parse packet data bytes")
	}

	packet, err := DeserializeGossipUDPPacket(packetData)
	if err != nil {
		return GossipMode{}, nil, err
	}

	membershipListRet := MembershipList{MemList: packet.MyMembershipListMap}
	return packet.MyGossipMode, &membershipListRet, nil
}

func SerializeGossipUDPPacket(packet *GossipUDPPacket) ([]byte, error) {
	binaryBuff := new(bytes.Buffer)
	encoder := gob.NewEncoder(binaryBuff)
	err := encoder.Encode(*packet)

	if err != nil {
		return nil, err
	}

	return binaryBuff.Bytes(), nil
}

func DeserializeGossipUDPPacket(data []byte) (*GossipUDPPacket, error) {
	var ret GossipUDPPacket
	byteBuffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(&ret)
	if err != nil {
		return &GossipUDPPacket{}, err
	}

	return &ret, nil
}
