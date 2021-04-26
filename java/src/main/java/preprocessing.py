lines = []
import gzip

with gzip.open('./gbooks', 'rb') as f:
    for i in range(8000000):
        lines.append(f.readline())

with gzip.open('./gbooks','wb') as f:
    for line in lines:
        f.write(line)
