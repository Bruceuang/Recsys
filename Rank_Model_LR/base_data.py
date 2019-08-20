import sys

user_action_data = '/root/7_codes/recsys_music/user_watch_pref.sml'
music_meta_data = '/root/7_codes/recsys_music/music_meta'
user_profile_data = '/root/7_codes/recsys_music/user_profile.data'

output_file = '../data/merge_base.data'


ofile = open(output_file, 'w')

item_info_dict = {}
with open(music_meta_data, 'r') as fd:
    for line in fd:
        ss = line.strip().split('\001')
        if len(ss) != 6:
            continue
        itemid, name, desc, total_timelen, location, tags = ss
        item_info_dict[itemid] = '\001'.join([name, desc, total_timelen, location, tags])

user_profile_dict = {}
with open(user_profile_data, 'r') as fd:
    for line in fd:
        ss = line.strip().split(',')
        if len(ss) != 5:
            continue
        userid, gender, age, salary, location = ss
        user_profile_dict[userid] = '\001'.join([gender, age, salary, location])


with open(user_action_data, 'r') as fd:
    for line in fd:
        ss = line.strip().split('\001')
        if len(ss) != 4:
            continue
        userid, itemid, watch_len, hour = ss

        if userid not in user_profile_dict:
            continue

        if itemid not in item_info_dict:
            continue

        ofile.write('\001'.join([userid, itemid, watch_len, hour, \
                user_profile_dict[userid], item_info_dict[itemid]]))
        ofile.write("\n")

ofile.close()