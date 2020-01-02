package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"
	"strconv"
	"github.com/spf13/cobra"
	log "github.com/sirupsen/logrus"
)

func fcntl(fd uintptr, cmd uintptr, arg uintptr) (val int, err error) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, fd, cmd, arg)
	val = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}

func isBlocking(fd uintptr) bool {
	val, err := fcntl(fd, syscall.F_GETFL, 0)
	if err != nil {
		panic(err)
	}
	return val & syscall.O_NONBLOCK == 0
}

func RunE(cmd *cobra.Command, args []string) error {

	verbosity, err := cmd.Flags().GetCount("verbose")
	if err != nil {
		return err
	}

	if verbosity > 0 {
		if verbosity >= 2 {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.InfoLevel)
		}
	} else {
		log.SetLevel(log.WarnLevel)
	}

	log.Debugf("args: %#v\n", args)

	port, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return err
	}
	command := args[1:]

	address, err := cmd.Flags().GetString("address")
	if err != nil {
		return err
	}

	ipv4, err := cmd.Flags().GetBool("ipv4")
	if err != nil {
		return err
	}

	ipv6, err := cmd.Flags().GetBool("ipv6")
	if err != nil {
		return err
	}

	load_average, err := cmd.Flags().GetFloat32("load-average")
	if err != nil {
		return err
	}

	processes, err := cmd.Flags().GetUint("processes")
	if err != nil {
		return err
	}

	si := &syscall.Sysinfo_t{}

	current_load := func() float32 {
		err = syscall.Sysinfo(si)
		if err != nil {
			log.Panicf("syscall.Sysinfo: %s\n", err.Error())
		}
		return float32(si.Loads[0]) / 65536.0
	}

	var network string
	if ipv4 && ipv6 {
		network = "tcp"
	} else if ipv4 {
		network = "tcp4"
	} else if ipv6 {
		network = "tcp6"
	} else {
		network = "tcp"
	}

	tcpAddr, err := net.ResolveTCPAddr(network, fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return err
	}

	l, err := net.ListenTCP(network, tcpAddr)
	if err != nil {
		return err
	}
	defer l.Close()

	processMap := make(map[int]*exec.Cmd)
	pidChan := make(chan int)
	acceptChan := make(chan *net.TCPConn)
	accepting := true

	acceptable_load := func() bool {
		return load_average == 0.0 || len(processMap) < 1 || current_load() < load_average
	}

	accept := func(l *net.TCPListener, c chan *net.TCPConn) {
		for {
			conn, err := l.AcceptTCP()
			if err != nil {
				log.Debugf("AcceptTCP: %s\n", err.Error())
				continue
			}
			c <- conn
			break
		}
	}

	go accept(l, acceptChan)

	for {
		select {
			case conn := <- acceptChan:
				log.Debug("request start\n")
				cmd, err := func(conn *net.TCPConn, command []string) (*exec.Cmd, error) {
					defer conn.Close()
					f, err := conn.File()
					if err != nil {
						return nil, err
					}
					defer f.Close()
					log.Debugf("isBlocking: %v\n", isBlocking(f.Fd()))
					cmd := exec.Command(command[0], command[1:]...)
					cmd.Stdin = f
					cmd.Stdout = f
					cmd.Stderr = os.Stderr
					err = cmd.Start()
					return cmd, err
				} (conn, command)

				if err != nil {
					return err
				}
				processMap[cmd.Process.Pid] = cmd

				go func (cmd *exec.Cmd, pidChan chan int) {
					cmd.Wait()
					pidChan <- cmd.Process.Pid
				} (cmd, pidChan)

				if uint(len(processMap)) == processes || !acceptable_load() {
					accepting = false
				} else {
					go accept(l, acceptChan)
				}
			case pid := <- pidChan:
				delete(processMap, pid)
				log.Debug("request end\n")
				if !accepting && acceptable_load() {
					accepting = true
					go accept(l, acceptChan)
				}
		}
	}
	return nil
}

func InitRootCmd() *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:   "socket-burst-dampener PORT CMD [ARG [ARG ...]]",
		Short: "A daemon that spawns a specified command to handle each connection, and dampens connection bursts",
		Args:  cobra.MinimumNArgs(2),
		RunE: RunE,
	}

	flags := rootCmd.PersistentFlags()

	flags.BoolP("help", "h", false, "show usage and exit")
	flags.String("address", "", "bind to the specified address")
	flags.Bool("ipv4", false, "prefer IPv4")
	flags.Bool("ipv6", false, "prefer IPv6")
	flags.Float32("load-average", 0.0, "don't accept multiple connections unless load is below")
	flags.Uint("processes", 1, "maximum number of concurrent processes, 0 means unbounded")
	flags.CountP("verbose", "v", "verbose logging (each occurence increases verbosity)")

	return rootCmd
}

func main() {
	rootCmd := InitRootCmd()
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
