"""Between jobs (untimed): if the VM balloon holds memory (MemAvailable low after a box restore/idle reclaim), touch memory
in 1 GiB steps so virtio_balloon deflates, then release it. Stops when MemAvailable >= target or after max steps."""
import sys, time
target_kb = float(sys.argv[1] if len(sys.argv) > 1 else 11.0) * 1024**2
def avail(): return int(open('/proc/meminfo').read().split('MemAvailable:')[1].split()[0])
b = []
for i in range(14):
    if avail() >= target_kb and not b: break
    if avail() < 600 * 1024: break          # never push the box into OOM
    b.append(bytearray(1024**3)); time.sleep(1.0)
del b; time.sleep(2)
print(round(avail() / 1024**2, 1))
