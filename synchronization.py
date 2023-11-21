import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import os
import pysrt

# CREATE DF WITH METADATA
con = sqlite3.connect("vods.db")
cur = con.cursor()

res = cur.execute("SELECT v.id, m.path, v.published_at, v.start, v.end, m.duration "
                  "FROM metadata m  JOIN vods v ON m.vod_id = v.id  "
                  "WHERE `group` = '2022-01-26_S1'")
data = res.fetchall()

# Create a DataFrame
columns = ['id', 'path', 'published_at', 'start_delta', 'end_delta', 'duration']
df = pd.DataFrame(data, columns=columns)


# Preprocess DataFrame
def start_calculator(row):
    if pd.notnull(row['start_delta']):
        return datetime.strptime(row['published_at'], '%Y-%m-%d %H:%M:%S.%f') + timedelta(seconds=row['start_delta'])


def end_calculator(row):
    if pd.notnull(row['duration']):
        return row['start_date'] + timedelta(seconds=row['duration'])


df['start_date'] = df.apply(start_calculator, axis=1)
df['end_date'] = df.apply(end_calculator, axis=1)
df['path'] = df['path'].apply(lambda x: x[39:-4])

# EXTRACT EVENT DATA FROM SRT FILES

streamers = os.listdir('srt')
if ".DS_Store" in streamers:
    streamers.remove(".DS_Store")
streamer_lobbies = {}
for streamer in streamers:
    subs = pysrt.open(f"srt/{streamer}")
    timestamps = []

    for sub in subs:
        event_time = timedelta(hours=sub.start.hours, minutes=sub.start.minutes, seconds=sub.start.seconds,
                               milliseconds=sub.start.milliseconds)
        if len(timestamps) == 0:
            if sub.text[:11] == 'Lobby start':
                timestamps.append([event_time])
            else:
                timestamps.append([None, event_time])
            continue
        if sub.text[:11] == 'Lobby start':
            if len(timestamps[-1]) == 1:
                timestamps[-1].append(None)
                timestamps.append([event_time])
            else:
                timestamps.append([event_time])
        elif sub.text[:9] == 'Lobby end':
            if len(timestamps[-1]) <= 1:
                timestamps[-1].append(event_time)
            else:
                timestamps.append([None, event_time])

    if len(timestamps[-1]) == 1:  # lonely start at the end
        timestamps[-1].append(None)
    streamer_lobbies[streamer] = timestamps

# MERGE SRT DATA AND METADATA IN DF

df['lobbies'] = df['path'].apply(lambda x: streamer_lobbies[f'{x}.srt'])


# Function to calculate the lobby timestamps
def sum_timedeltas(row):
    return [[row['start_date'] + td if td is not None else None
             for td in sublist] for sublist in row['lobbies']]


# Apply the function to create a new column 'SumTimedeltas'
df['lobbies_times'] = df.apply(sum_timedeltas, axis=1)

# create list of all the lobbies times
df_exploded = df.explode('lobbies_times')
all_lobbies_list = df_exploded['lobbies_times'].tolist()
sorted_all_lobbies_list = sorted(all_lobbies_list, key=lambda x: (x[0] is None, x[0]))

# assign lobby times to a lobby dictionary
lobby_dic = {}
num_counter = 1
for lob in sorted_all_lobbies_list:
    if lob[0] is None or lob[1] is None:
        continue
    assigned_to_lobby = False
    for i in lobby_dic.keys():
        if lob[0] - lobby_dic[i]['lobby_start'] < timedelta(
                minutes=2):  # and lob[1] - lobby_dic[i]['lobby_end'] < timedelta(minutes=2):
            lobby_dic[i]['timestamp_list'].append(lob)
            lobby_dic[i]['lobby_start'] = lob[0]
            lobby_dic[i]['lobby_end'] = lob[1]
            assigned_to_lobby = True
            break
    if assigned_to_lobby:
        continue
    else:
        lobby_dic[num_counter] = {'lobby_start': lob[0],
                                  'lobby_end': lob[1],
                                  'timestamp_list': [lob]}
        num_counter += 1

# to assign lobby times with None as start value extract median lobby start / end for built lobbies
for i in lobby_dic.keys():
    lobby_starts = sorted([x[0] for x in lobby_dic[i]['timestamp_list']])
    lobby_dic[i]['lobby_start'] = lobby_starts[int(len(lobby_starts) / 2) - 1]
    lobby_ends = sorted([x[1] for x in lobby_dic[i]['timestamp_list']])
    lobby_dic[i]['lobby_end'] = lobby_ends[int(len(lobby_ends) / 2) - 1]

# now assign lobby times with None to lobbies
for lob in sorted_all_lobbies_list:
    if lob[0] is None or lob[1] is None:
        assigned = False
        if lob[0] is None:
            for i in lobby_dic.keys():
                if abs(lob[1] - lobby_dic[i]['lobby_end']) < timedelta(minutes=2):
                    lobby_dic[i]['timestamp_list'].append(lob)
                    assigned = True
                    break
            if not assigned:
                print(lob)
        elif lob[1] is None:
            for i in lobby_dic.keys():
                if abs(lob[0] - lobby_dic[i]['lobby_start']) < timedelta(minutes=2):
                    lobby_dic[i]['timestamp_list'].append(lob)
                    assigned = True
                    break
            if not assigned:
                print(lob)
    else:
        continue


# delete all lobbies which have only one timestamp which is also shorter than one minute
del_list = []
for k, v in lobby_dic.items():
    if len(v['timestamp_list']) == 1:
        try:
            short_lobby = (v['timestamp_list'][0][1] - v['timestamp_list'][0][0]) < timedelta(minutes=1)
            if short_lobby:
                del_list.append(k)
        except:
            continue
for k in del_list:
    lobby_dic.pop(k)
# restore order in lobby names such that one does not skip some numbers
keys = sorted(list(lobby_dic.keys()))
new_dic = {}
c = 1
for i in keys:
    new_dic[c] = lobby_dic[i]
    c += 1
lobby_dic = new_dic



# create new column in df in which lobby times are mapped to lobbies
def find_lobby(sublist):
    for k, v in lobby_dic.items():
        if sublist in v['timestamp_list']:
            return k
    return None


df['lobbies_assigned_with_None'] = df['lobbies_times'].apply(lambda sublists: [find_lobby(sublist) for sublist in sublists])


# kick all lobby times which are assigned to no lobby and which lie between two subsequent lobbies
def kick_none_lobbies(row, base_column):
    kick_list = []
    for ind, v in enumerate(row['lobbies_assigned_with_None']):
        if v == None:
            try:
                diff = row['lobbies_assigned_with_None'][ind + 1] - row['lobbies_assigned_with_None'][ind - 1]
                if diff == 1:
                    kick_list.append(ind)
            except: continue
    c_list = row[base_column][:]
    for ind in sorted(kick_list, reverse=True):
        c_list.pop(ind)
    return c_list


df['lobbies_assigned'] = df.apply(lambda row: kick_none_lobbies(row, 'lobbies_assigned_with_None'), axis=1)
df['lobbies_times_assigned'] = df.apply(lambda row: kick_none_lobbies(row, 'lobbies_times'), axis=1)


# when for a streamer two lobby times are assigned to the same lobby, check if second one has no start timestamp and if so merge
def merge_lobbies(row):
    unique_set = set()
    duplicates_lobbies = set(x for x in row['lobbies_assigned'] if x in unique_set or unique_set.add(x))
    duplicates_lobbies = sorted(list(duplicates_lobbies))
    indices = []
    for i in duplicates_lobbies:
        indices.append(row['lobbies_assigned'].index(i))
    indices = sorted(indices, reverse=True)
    times_final = row['lobbies_times_assigned'].copy()
    lobbies_final = row['lobbies_assigned'].copy()
    for ind in indices:
        if times_final[ind+1][0] is None and times_final[ind][0] is not None and times_final[ind][1] is not None:
            new_timestamp = [times_final[ind][0], times_final[ind+1][1]]
            times_final.pop(ind+1)
            times_final.pop(ind)
            lobbies_final.pop(ind + 1)
            times_final.insert(ind, new_timestamp)
    return times_final, lobbies_final


df[['lobbies_times_final', 'lobbies_assigned_final']] = df.apply(lambda row: merge_lobbies(row), axis=1, result_type='expand')


# check for all final lobbies for outliers within the lobbies
def checker(row, num):
    try:
        ind = row['lobbies_assigned_final'].index(num)
    except:
        ind = None
    if ind is not None:
        return row["lobbies_times_final"][ind]
    else:
        return None


number_extracted_lobbies = max(df['lobbies_assigned_final'].apply(lambda x: max(x)))
for lobby_num in range(1, number_extracted_lobbies):

    r = df.apply(lambda row: checker(row, lobby_num), axis=1)

    # delete all streamers which did not join the lobby and thus have None in this row
    del_list = []
    for i in range(len(r)):
        if r[i] is None:
            del_list.append(i)
    del_list = sorted(del_list, reverse= True)
    for ind in del_list:
        r.drop(ind, inplace=True)
    r.index = list(range(len(r)))

    for i in range(len(r)):
        c = 0
        for j in range(len(r)):  # check start time of event
            if r[i][0] is not None and r[j][0] is not None:
                if abs(r[i][0] - r[j][0]) > timedelta(seconds=15):
                    c += 1
        if c >= 2:
            print(f'Lobby number: {lobby_num}, id: {i},  start - {r[i]}')

        c = 0
        for j in range(len(r)):  # check end time of event
            if r[i][1] is not None and r[j][1] is not None:
                if abs(r[i][1] - r[j][1]) > timedelta(seconds=15):
                    c += 1
        if c >= 2:
            print(f'Lobby number: {lobby_num}, id: {i}, end - {r[i]}')
