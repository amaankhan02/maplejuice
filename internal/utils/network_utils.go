package utils

import (
	"cs425_mp4/internal/config"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

// Returns the VM number based on the hostname of this machine, and also returns the hostname itself
// In the result of an error, return 0 for VM number and empty string for hostname
func GetLocalVMInfo() (int, string) {
	// get the hostname of this machine
	hostname, err_hostname := os.Hostname() // to make sure we don't add our hostname as the peer
	if err_hostname != nil {
		log.Fatal("Failed to retrieve machine hostname")
	}

	vm_num, err_vm_num := GetVMNumber(hostname)
	if err_vm_num != nil {
		return 0, ""
	}

	return vm_num, hostname
}

func GetVMNumber(hostname string) (int, error) {
	vm_num, err_vm_num := strconv.Atoi(hostname[config.VM_NUMBER_START:config.VM_NUM_END])
	if err_vm_num != nil {
		return 0, errors.New("Invalid hostname to parse into a vm number")
	} else {
		return vm_num, nil
	}
}

// Get hostname of a machine of a certain vm number
func GetHostname(vmNumber int) string {
	return fmt.Sprintf(config.HOSTNAME_FORMAT, vmNumber)
}

// Get the IP address of a machine with a certain hostname
func GetIP(hostname string) string {
	ipAddr, err_ip := net.LookupIP(hostname)
	if err_ip != nil {
		log.Fatalf("Failed to resolve IP addresses for %s: %v\n", hostname, err_ip)
	}

	return ipAddr[0].String()
}

/*
Return the port assigned for VM vmNumber in the format 80__
vmNumber should be an integer in the range [1, 9] inclusive
*/
func GetGossipPort(vmNumber int) string {
	return fmt.Sprintf(config.GOSSIP_PORT_FORMAT, vmNumber)
}

func GetSDFSPort(vmNumber int) string {
	return fmt.Sprintf(config.SDFS_TCP_PORT_FORMAT, vmNumber)
}
