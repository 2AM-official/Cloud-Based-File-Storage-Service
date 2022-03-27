
import sys
x = sys.argv[1]
f = open(f"statics/{x}","w")
for _ in range(100000):
	f.write(2000*x)
f.close()