#map
import sys
for line in sys.stdin:
    i_a, i_b, s = line.strip().split('\t')
    print("%s\t%s" % (i_a + "^A" + i_b, s))