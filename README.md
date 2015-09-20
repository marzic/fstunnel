# fstunnel
TCP tunnel over the filesystem. Useful for getting out of VMs running VPNs that take over all network routes.

Tunnels TCP traffic from point `a.py` to point `b.py` (back and forth).

##### Two Python3 scripts (a.py and b.py) make up the TCP tunnel:
---
1. `a.py` reads from the source socket and writes the data as files to the `b` directory
2. `b.py` reads files from the `b` directory and writes them out to the destination socket
3. `b.py` reads the response from the destination socket and writes the data as files to the `a` directory
3. `a.py` reads the response files from the `a` directory and writes them out to the source socket

##### Example: using a host machine browser and intercepting HTTP proxy while going through a guest VM's VPN
---
- put the fstunnel directory in a VM shared directory
- point the host browser to use the host HTTP proxy
- point the host intercepting HTTP proxy to an upstream HTTP proxy at 127.0.0.1:2222
- tell a guest VM HTTP proxy to listen on 127.0.0.1:3333
- on the host: `$ python a.py 127.0.0.1 2222`
- in the guest VM: `$ python b.py 127.0.0.1 3333`

Now we have: host browser -> host intercepting HTTP proxy -> fstunnel -> guest VM HTTP proxy -> VPN
