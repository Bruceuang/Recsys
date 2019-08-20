#reduce
import sys

cur_ii_pair = None
score = 0.0

for line in sys.stdin:
    ii_pair, s = line.strip().split('\t')
    if not cur_ii_pair:
        cur_ii_pair = ii_pair
    if ii_pair != cur_ii_pair:
        ss = cur_ii_pair.split('^A')
        if len(ss) != 2:
            continue
        item_a, item_b = ss
        print("%s\t%s\t%s" % (item_a, item_b, score))
        cur_ii_pair = ii_pair
        score = 0.0
        
    score += float(s)
    
ss = cur_ii_pair.spliut('^A')
if len(ss) != 2:
    sys.exit()
item_a, item_b = ss
print("%s\t%s\t%s" % (item_a, item_b, score))