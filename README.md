# socket-burst-dampener

A daemon that spawns a specified command to handle each connection, and
dampens connection bursts.

## Motivation
It is typical to configure a forking daemon such as rsync so that it
will respond with an error when a maximum number of concurrent
connections has been exceeded. Alternatively, it may be desirable to
queue excess connections, and service them in the order of arrival
while taking care to ensure that too many processes are not spawned at
once. The socket-burst-dampener daemon applies this behavior to any
daemon command that works with inetd.

On Linux, the net.core.somaxconn sysctl setting specifies the queue
length for completely established sockets waiting to be accepted.
It may also be useful to adjust the maximum queue length for incomplete
sockets that is controlled by the net.ipv4.tcp_max_syn_backlog sysctl
setting (mentioned in the
[listen(2)](http://man7.org/linux/man-pages/man2/listen.2.html) man page).

## Usage
```
Usage:
  socket-burst-dampener PORT CMD [ARG [ARG ...]] [flags]

Flags:
      --address string         bind to the specified address
  -h, --help                   show usage and exit
      --ipv4                   prefer IPv4
      --ipv6                   prefer IPv6
      --load-average float32   don't accept multiple connections unless load is below
      --processes uint         maximum number of concurrent processes, 0 means unbounded (default 1)
  -v, --verbose count          verbose logging (each occurence increases verbosity)
```
## Example with rsync
```
socket-burst-dampener 873 --processes $(nproc) --load-average $(nproc) -- rsync --daemon
```
