import sys

input_file = "../data/merge_base.data"

output_file = '../data/cf_train.data'
ofile = open(output_file, 'w')

key_dict = {}
with open(input_file, 'r') as fd:
    for line in fd:
        ss = line.strip().split('\001')
        userid = ss[0].strip()
        itemid = ss[1].strip()
        watch_len = ss[2].strip()
        hour = ss[3].strip()
        gender = ss[4].strip()
        age = ss[5].strip()
        salary = ss[6].strip()
        user_location = ss[7].strip()
        name = ss[8].strip()
        desc = ss[9].strip()
        total_timelen = ss[10].strip()
        item_location = ss[11].strip()
        tags = ss[12].strip()

        key = '_'.join([userid, itemid])
        if key not in key_dict:
            key_dict[key] = []
        key_dict[key].append((int(watch_len), int(total_timelen)))

for k, v in key_dict.items():
    t_finished = 0
    t_all = 0
    for vv in v:
        t_finished += vv[0]
        t_all += vv[1]

    score = float(t_finished) / float(t_all)
    userid, itemid = k.strip().split('_')


    ofile.write(','.join([userid, itemid, str(score)]))
    ofile.write("\n")

ofile.close()