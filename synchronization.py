import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import os
import pysrt
from pysrt import SubRipTime
import sys
import pickle

sessions = os.listdir("../../../dimstore/pop520978/data")
sessions.remove('unsorted')
sessions.remove('copy_files.sh')


for session in sessions:
    with open(f'{session}.txt', 'w') as file:
        sys.stdout = file

        print(f"SESSION: {session}")
        print("\n\n\n")

        try:
            delete_streamer = True
            delete_streamer_list = []
            while delete_streamer:

                # CREATE DF WITH METADATA
                con = sqlite3.connect("vods.db")
                cur = con.cursor()
                g = session
                res = cur.execute("SELECT v.id, m.path, v.published_at, v.start, v.end, m.duration "
                                  "FROM metadata m  JOIN vods v ON m.vod_id = v.id  "
                                  "WHERE `group` = ?", (g,))
                data = res.fetchall()
                for name in delete_streamer_list:
                    data = [tup for tup in data if tup[1][39:] != f"{name}.mkv"]


                # Create a DataFrame
                columns = ['id', 'path', 'published_at', 'start_delta', 'end_delta', 'duration']
                df = pd.DataFrame(data, columns=columns)


                # Preprocess DataFrame
                def start_calculator(row):
                    if pd.notnull(row['start_delta']):
                        return datetime.strptime(row['published_at'], '%Y-%m-%d %H:%M:%S.%f') + timedelta(seconds=row['start_delta'])
                    else:
                        return datetime.strptime(row['published_at'], '%Y-%m-%d %H:%M:%S.%f')


                def end_calculator(row):
                    if pd.notnull(row['duration']):
                        return row['start_date'] + timedelta(seconds=row['duration'])
                    else:
                        print(f'There is a streamer without duration: {row["path"]}')
                        print("\n\n\n")


                df['start_date'] = df.apply(start_calculator, axis=1)
                df['end_date'] = df.apply(end_calculator, axis=1)
                df['path'] = df['path'].apply(lambda x: x[39:-4])

                # EXTRACT EVENT DATA FROM SRT FILES
                srt_path = f'../../../dimstore/pop520978/data/{session}/srt'
                for name in delete_streamer_list:
                    if os.path.exists(f'{srt_path}/{name}.srt'):
                        os.remove(f'{srt_path}/{name}.srt')
                streamers = os.listdir(srt_path)
                if ".DS_Store" in streamers:
                    streamers.remove(".DS_Store")

                # remove all streamers for which i do not have a srt file
                def delete_streamer_without_srt(path):
                    if f'{path}.srt' in streamers:
                        return 0
                    else:
                        print(f"Streamer without srt file: {path}")
                        print("\n\n\n")
                        return 1

                df['drop'] = df['path'].apply(lambda x: delete_streamer_without_srt(x))
                df = df[df['drop'] == 0]
                df = df.drop('drop', axis=1)
                df = df.reset_index(drop=True)

                streamer_lobbies = {}
                for streamer in streamers:
                    subs = pysrt.open(f"{srt_path}/{streamer}")
                    timestamps = []

                    for sub in subs:
                        if sub.duration > SubRipTime(seconds=30) or sub.duration < SubRipTime(seconds=2):
                            continue
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


                # CHANGE FORMAT OF LOBBY TIMES IN DF TO TIMESTAMPS
                # Function to calculate the lobby timestamps
                def sum_timedeltas(row):
                    return [[row['start_date'] + td if td is not None else None
                             for td in sublist] for sublist in row['lobbies']]


                # Apply the function to create a new column 'lobbies_times'
                df['lobbies_times'] = df.apply(sum_timedeltas, axis=1)

                # kick all lobbies times with a duration < 1 minute (assumption that lobbies usually go longer and no problem if data of short lobby is lost)
                df['lobbies_times'] = df['lobbies_times'].apply(
                    lambda lst: [x for x in lst if x[1] is None or x[0] is None or (x[1] - x[0]) > timedelta(seconds=60)])

                # CREATE A DICTIONARY OF LOBBIES TO ASSIGN DIFFERENT LOBBY TIMES TO LOBBIES
                # create list of all the lobbies times
                df_exploded = df.explode('lobbies_times')
                all_lobbies_list = df_exploded['lobbies_times'].tolist()
                sorted_all_lobbies_list = sorted(all_lobbies_list, key=lambda x: (x[0] is None, x[0]))

                # assign lobby times to a lobby dictionary
                lobby_dic = {}
                num_counter = 1
                for lob in sorted_all_lobbies_list:
                    if lob[0] is None or lob[1] is None:  # lobbies containing None will be assigned later on
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


                # ASSIGN LOBBY TIMES CONTAINING A NONE VALUE
                # to assign lobby times with None as start value extract median lobby start / end for built lobbies
                for i in lobby_dic.keys():
                    lobby_starts = sorted([x[0] for x in lobby_dic[i]['timestamp_list']])
                    lobby_dic[i]['lobby_start'] = lobby_starts[int(len(lobby_starts) / 2) - 1]
                    lobby_ends = sorted([x[1] for x in lobby_dic[i]['timestamp_list']])
                    lobby_dic[i]['lobby_end'] = lobby_ends[int(len(lobby_ends) / 2) - 1]

                # now assign lobby times with None to lobbies
                for lob in sorted_all_lobbies_list:
                    if lob[0] is None or lob[1] is None:
                        if lob[0] is None:
                            for i in lobby_dic.keys():
                                if abs(lob[1] - lobby_dic[i]['lobby_end']) < timedelta(minutes=2):
                                    lobby_dic[i]['timestamp_list'].append(lob)
                                    assigned = True
                                    break

                        elif lob[1] is None:
                            for i in lobby_dic.keys():
                                if abs(lob[0] - lobby_dic[i]['lobby_start']) < timedelta(minutes=2):
                                    lobby_dic[i]['timestamp_list'].append(lob)
                                    assigned = True
                                    break

                    else:
                        continue


                # DELETE "WRONG" LOBBIES
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


                # CREATE COLUMN IN DF MAPPING LOBBY TIMES TO LOBBIES
                # create new column in df in which lobby times are mapped to lobbies
                def find_lobby(sublist):
                    for k, v in lobby_dic.items():
                        if sublist in v['timestamp_list']:
                            return k
                    return None


                df['lobbies_assigned_with_None'] = df['lobbies_times'].apply(lambda sublists: [find_lobby(sublist) for sublist in sublists])


                # check for streamers where event extraction did not work: more than 50% of extracted lobby times are assigned to lobbies with no other lobby times
                def single_lobbies_counter(row):
                    counter = 0
                    for lobby in row['lobbies_assigned_with_None']:
                        if lobby is None:
                            counter += 1
                        elif len(lobby_dic[lobby]['timestamp_list']) == 1:
                            counter += 1
                    return counter / len(row['lobbies_assigned_with_None'])


                df['single_lobby_score'] = df.apply(lambda row: single_lobbies_counter(row), axis=1)

                delete_streamer = False
                delete_streamer_list = delete_streamer_list + list(df.loc[df['single_lobby_score'] > 0.5, 'path'].values)
                if len(list(df.loc[df['single_lobby_score'] > 0.5, 'path'].values)) > 0:
                    delete_streamer = True


            # merge lobbies according to end timestamps
            for h in sorted(list(lobby_dic.keys()), reverse=True):
                for j in sorted(list(lobby_dic.keys()), reverse=True):
                    if j == h:
                        continue
                    elif abs(lobby_dic[j]['lobby_end'] - lobby_dic[h]['lobby_end']) < timedelta(seconds=45):
                        lobby_dic[j]['timestamp_list'] = lobby_dic[j]['timestamp_list'] + lobby_dic[h]['timestamp_list']
                        lobby_dic[h]['lobby_end'] = pd.Timestamp('2099-01-01 00:00:01')
                        df['lobbies_assigned_with_None'] = df['lobbies_assigned_with_None'].apply(lambda lst: [j if x == h else x for x in lst])
                        break

            # recreate order in lobby dic
            del_list = []
            for k in lobby_dic.keys():
                if lobby_dic[k]['lobby_end'] == pd.Timestamp('2099-01-01 00:00:01'):
                    del_list.append(k)
            for k in del_list:
                lobby_dic.pop(k)
            # restore order in dic and create mapper for df
            keys = sorted(list(lobby_dic.keys()))
            new_dic = {}
            mapper = {}
            c = 1
            for i in keys:
                new_dic[c] = lobby_dic[i]
                mapper[i] = c
                c += 1
            lobby_dic = new_dic
            # restore order in df using mapper
            for source in sorted(list(mapper.keys())):
                df['lobbies_assigned_with_None'] = df['lobbies_assigned_with_None'].apply(
                    lambda lst: [mapper[source] if x == source else x for x in lst])


            # when lobbies are wrongly ordered time-wise (lobby_n has later end than lobby_n+1) find lobby time causing this and split this lobby time up into two lobbies
            # first case: second lobby is causing the problem because it has only one lobby time ending too early -> move this lobby time to prior lobby and delete later lobby
            wrong_lobbies = []
            deleted_lobbies = []
            keys = list(lobby_dic.keys())
            for i in range(1, len(keys)):
                if lobby_dic[keys[i]]['lobby_end'] < lobby_dic[keys[i - 1]]['lobby_end']:
                    wrong_lobbies.append(keys[i - 1])
            for wl in wrong_lobbies:
                if len(lobby_dic[wl+1]['timestamp_list']) == 1:
                    wrong_timestamp = lobby_dic[wl+1]['timestamp_list'][0]
                    id = df[df['lobbies_times'].apply(
                        lambda lst: wrong_timestamp in lst)]['id']  # id of streamers belonging to this lobby time
                    for streamer in id.values:
                        old_lobby_times = list(df.loc[df['id'] == streamer, 'lobbies_times'])[0]
                        old_lobby_assignments = list(df.loc[df['id'] == streamer, 'lobbies_assigned_with_None'])[0]
                        for index, j in enumerate(old_lobby_times):
                            if j == wrong_timestamp:
                                break
                        # now adjust values in df for causing streamer
                        old_lobby_assignments.insert(index,
                                                     wl)
                        old_lobby_assignments.remove(wl+1)

                        # now add lobby time to correct lobby in lobby dic and then delete wrong lobby
                        lobby_dic[wl]['timestamp_list'].append(wrong_timestamp)
                        lobby_dic.pop(wl+1)
                        deleted_lobbies.append(wl+1)

            # now recreate order in lobby dic and df because of deleted lobbies
            # restore order in dic and create mapper for df
            keys = sorted(list(lobby_dic.keys()))
            new_dic = {}
            mapper = {}
            c = 1
            for i in keys:
                new_dic[c] = lobby_dic[i]
                mapper[i] = c
                c += 1
            lobby_dic = new_dic
            # restore order in df using mapper
            for source in sorted(list(mapper.keys())):
                df['lobbies_assigned_with_None'] = df['lobbies_assigned_with_None'].apply(
                    lambda lst: [mapper[source] if x == source else x for x in lst])

            # second case: first lobby is causing the problem: only one lobby time ending too late -> split this lobby time up into two lobby times with None's (one in each lobby), then change order of lobbies to be correct
            # first identify these lobbies
            wrong_lobbies = []
            keys = list(lobby_dic.keys())
            for i in range(1, len(keys)):
                if lobby_dic[keys[i]]['lobby_end'] < lobby_dic[keys[i-1]]['lobby_end']:
                    wrong_lobbies.append(keys[i-1])
            for wl in wrong_lobbies:
                # identify timestamp causing error in this lobby
                for l_times in lobby_dic[wl]['timestamp_list']:
                    if l_times[0] == lobby_dic[wl]['lobby_start'] and l_times[1] == lobby_dic[wl]['lobby_end']:
                        wrong_timestamp = l_times
                        break
                id = df[df['lobbies_times'].apply(
                    lambda lst: wrong_timestamp in lst)]['id']  # id of streamers belonging to this lobby time
                for streamer in id.values:
                    old_lobby_times = list(df.loc[df['id'] == streamer, 'lobbies_times'])[0]
                    old_lobby_assignments = list(df.loc[df['id'] == streamer, 'lobbies_assigned_with_None'])[0]
                    for index, j in enumerate(old_lobby_times):
                        if j == l_times:
                            break
                    # now adjust values in df for causing streamer
                    old_lobby_assignments.insert(index, wl+1)  # here just adding new lobby as old lobby assignment can remain just one shifted (inserted at wrong position because in following list comprehension for all streamers this is accounted for
                    old_lobby_times.insert(index, [l_times[0], None])  # these operations are applied in the df as well as pointer is still on the list
                    old_lobby_times.insert(index + 1, [None, l_times[1]])
                    old_lobby_times.remove(l_times)
                    # adjust values in df for all other streamers: assign wrong lobby with correct number and following lobby with number of wrong lobby
                    df['lobbies_assigned_with_None'] = df['lobbies_assigned_with_None'].apply(
                        lambda lst: [wl + 1 if x == wl else (wl if x == wl + 1 else x) for x in lst])

                    # remove wrong timestamp and add new timestamps also from / to lobby_dic
                    lobby_dic[wl]['timestamp_list'].remove(l_times)
                    lobby_dic[wl + 1]['timestamp_list'].append([l_times[0], None])
                    lobby_dic[wl]['timestamp_list'].append([None, l_times[1]])


            # now adjust lobby dic
            for wl in wrong_lobbies:
                wrong_lobby_values = lobby_dic[wl]
                other_lobby_values = lobby_dic[wl + 1]
                lobby_dic[wl] = other_lobby_values
                lobby_dic[wl + 1] = wrong_lobby_values
                # calculate new start and end values for wrong lobby
                lobby_starts = sorted([x[0] for x in lobby_dic[wl + 1]['timestamp_list'] if x[0] is not None])
                if len(lobby_starts) != 0:
                    lobby_dic[wl+1]['lobby_start'] = lobby_starts[int(len(lobby_starts) / 2) - 1]
                else:
                    lobby_dic[wl+1]['lobby_start'] = None

                lobby_ends = sorted([x[1] for x in lobby_dic[wl + 1]['timestamp_list'] if x[1] is not None])

                if len(lobby_ends) != 0:
                    lobby_dic[wl+1]['lobby_end'] = lobby_ends[int(len(lobby_ends) / 2) - 1]
                else:
                    lobby_dic[wl+1]['lobby_end'] = None

            # IMPROVE LOBBY ASSIGNMENTS BY APPLYING SOME RULES
            # kick all lobby times which are assigned to no lobby and which lie between two subsequent lobbies (only lobby times containing at least one None are not assigned to a lobby)
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


            # when for a streamer two lobby times are assigned to the same lobby, check if second one has no start timestamp or first one has no end timestamp and if so merge
            def merge_lobbies(row, lobbies_column, lobbies_times_column):
                unique_set = set()
                duplicates_lobbies = set(x for x in row[lobbies_column] if x in unique_set or unique_set.add(x))
                if None in duplicates_lobbies:
                    duplicates_lobbies.remove(None)
                duplicates_lobbies = sorted(list(duplicates_lobbies))
                indices = []
                for i in duplicates_lobbies:
                    indices.append(row[lobbies_column].index(i))
                indices = sorted(indices, reverse=True)
                times_final = row[lobbies_times_column].copy()
                lobbies_final = row[lobbies_column].copy()
                for ind in indices:
                    if (times_final[ind+1][0] is None and times_final[ind][0] is not None and times_final[ind][1] is not None)\
                            or (times_final[ind][1] is None and times_final[ind][0] is not None and times_final[ind+1][1] is not None)\
                            or (times_final[ind][0] is None and times_final[ind][1] is not None and times_final[ind+1][1] is not None):
                        new_timestamp = [times_final[ind][0], times_final[ind+1][1]]
                        times_final.pop(ind+1)
                        times_final.pop(ind)
                        lobbies_final.pop(ind + 1)
                        times_final.insert(ind, new_timestamp)
                return times_final, lobbies_final


            df[['lobbies_times_final', 'lobbies_assigned_final']] = df.apply(lambda row: merge_lobbies(row, 'lobbies_assigned', 'lobbies_times_assigned'), axis=1, result_type='expand')
            for counter in range(3):  # in case there are multiple lobby times for one streamer assigned to same lobby. I start with two last ones and then move to front. So if lobby time 2,3,4 are assigned to lobby 8, i start with merging 3&4 and then 2&the merged lobby times
                # Now new columns as input parameters as the first applied merge was stored in these columns.
                df[['lobbies_times_final', 'lobbies_assigned_final']] = df.apply(
                    lambda row: merge_lobbies(row, 'lobbies_assigned_final', 'lobbies_times_final'), axis=1, result_type='expand')

            """
            # remove timestamps of lobby outliers with weird end times caused by very long duration of end event in srt event
            # create streamer duration dictionary to check for detected end time outliers whether their srt end event is suspiciously long
            streamer_duration_lobbies = {}
            for streamer in streamers:
                subs = pysrt.open(f"{srt_path}/{streamer}")
                timestamps = []
            
                for sub in subs:
                    if len(timestamps) == 0:
                        if sub.text[:11] == 'Lobby start':
                            timestamps.append([sub.duration])
                        else:
                            timestamps.append([None, sub.duration])
                        continue
                    if sub.text[:11] == 'Lobby start':
                        if len(timestamps[-1]) == 1:
                            timestamps[-1].append(None)
                            timestamps.append([sub.duration])
                        else:
                            timestamps.append([sub.duration])
                    elif sub.text[:9] == 'Lobby end':
                        if len(timestamps[-1]) <= 1:
                            timestamps[-1].append(sub.duration)
                        else:
                            timestamps.append([None, sub.duration])
            
                if len(timestamps[-1]) == 1:  # lonely start at the end
                    timestamps[-1].append(None)
                streamer_duration_lobbies[streamer] = timestamps
            """
            # go over all lobbies and for each lobby find those lobby times which have an end time that is more than 15 sec away from at least two other end times in this lobby
            def checker(row, num):
                try:
                    ind = row['lobbies_assigned_final'].index(num)
                except:
                    ind = None
                if ind is not None:
                    return row["lobbies_times_final"][ind]
                else:
                    return None

            """
            number_extracted_lobbies = max(df['lobbies_assigned_final'].apply(lambda x: max(x)))
            for lobby_num in range(1, number_extracted_lobbies):
            
                r = df.apply(lambda row: checker(row, lobby_num), axis=1)
            
                # delete all streamers which did not join the lobby and thus have None in this row
                del_list = []
                for i in range(len(r)):
                    if r[i] is None:
                        del_list.append(i)
                del_list = sorted(del_list, reverse=True)
                for ind in del_list:
                    r.drop(ind, inplace=True)
                streamer_lookup = list(r.index)
                r.index = list(range(len(r)))
            
                for i in range(len(r)):
                    c = 0
                    for j in range(len(r)):  # check end time of event
                        if r[i][1] is not None and r[j][1] is not None:
                            if abs(r[i][1] - r[j][1]) > timedelta(seconds=15):
                                c += 1
                    if c >= 2:  # if at least two other lobby ends in this lobby are more than 15 sec away check duration of end srt event
                        idx_in_streamer_duration_dic = list(df[df["path"] == df["path"][streamer_lookup[i]]]["lobbies_times"])[0].index(r[i])
                        if streamer_duration_lobbies[f'{df["path"][streamer_lookup[i]]}.srt'][idx_in_streamer_duration_dic][1] > SubRipTime(seconds=30):  # if end event duration is larger than 30 sec set its timestamp to None
                            idx = list(df[df["path"] == df["path"][streamer_lookup[i]]]["lobbies_times_final"])[0].index(r[i])  # find position in lobby times final list
                            def gg(row, sn, li):
                                if row['path'] == sn:
                                    row['lobbies_times_final'][li] = None
                            df.apply(lambda row: gg(row, df["path"][streamer_lookup[i]], idx), axis=1)
            
            """


            # EVALUATION OF LOBBY ASSIGNMENTS
            # check for all final lobbies for outliers within the lobbies
            """
            number_extracted_lobbies = max(df['lobbies_assigned_final'].apply(lambda x: max(x, key=lambda y: float('-inf') if y is None else y)))
            for lobby_num in range(1, number_extracted_lobbies):
            
                r = df.apply(lambda row: checker(row, lobby_num), axis=1)
            
                # delete all streamers which did not join the lobby and thus have None in this row
                del_list = []
                for i in range(len(r)):
                    if r[i] is None:
                        del_list.append(i)
                del_list = sorted(del_list, reverse=True)
                for ind in del_list:
                    r.drop(ind, inplace=True)
                streamer_lookup = list(r.index)
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
            """

            # check for all streamers if final lobbies assigned are subsequent numbers
            print("THE FOLLOWING STREAMERS HAVE NON-CONSECUTIVE LOBBIES:")
            for j in range(len(df)):
                lobbies = df['lobbies_assigned_final'][j]
                for i in range(len(lobbies) - 1):
                    if lobbies[i] is None:
                        print(f'Streamer: {df["path"][j]}: Lobbies numbers not consecutive: {lobbies[i]} and {lobbies[i+1]}')
                    elif lobbies[i] + 1 != lobbies[i + 1]:
                        print(f'Streamer: {df["path"][j]}: Lobbies numbers not consecutive: {lobbies[i]} and {lobbies[i+1]}')


            #  for all lobbies find trustworthy lobby times
            for key in lobby_dic.keys():
                lobby_dic[key]['trustworthy_times'] = []
                first_lobby_times = []  # only consider lobby times which originally created lobby as candidates for trustworthy lobby times
                for lt in lobby_dic[key]['timestamp_list']:
                    if lt[0] is None or lt[1] is None:
                        break
                    else:
                        first_lobby_times.append(lt)
                for lt in first_lobby_times:
                    c = 0
                    for olt in first_lobby_times:
                        if abs((lt[1] - lt[0]) - (olt[1] - olt[0])) < timedelta(seconds=5) and abs(lt[1] - olt[1]) < timedelta(seconds=10) and abs(lt[0] - olt[0]) < timedelta(seconds=10):  # compare duration and start / end timestamp with other candidates -> trustworthy if one other candidate has similar values
                            c += 1
                    if c >= 2:
                        lobby_dic[key]['trustworthy_times'].append(lt)


            # for streamers check which of their lobby times is trustworthy
            trustworthy_streamer_dic = {}


            def extract_trustworthy_lobby_times(row):
                streamer = row['path']
                trustworthy_streamer_dic[streamer] = {}
                for lobby_index, lobby_time in enumerate(row['lobbies_times_final']):
                    if row['lobbies_assigned_final'][lobby_index] is None:
                        continue
                    elif lobby_time in lobby_dic[row['lobbies_assigned_final'][lobby_index]]['trustworthy_times']:
                        trustworthy_streamer_dic[streamer][row['lobbies_assigned_final'][lobby_index]] = lobby_time


            df.apply(lambda row: extract_trustworthy_lobby_times(row), axis=1)

            """
            # calculate average streamer differences
            streamer_utc_diff = {}
            streamers = trustworthy_streamer_dic.keys()
            for s1 in streamers:
                streamer_utc_diff[s1] = {}
                s1_lobbies = list(trustworthy_streamer_dic[s1].keys())
                for s2 in streamers:
                    if s1 == s2:
                        continue
                    s2_lobbies = list(trustworthy_streamer_dic[s2].keys())
                    both_streamer_lobbies = list(set(s1_lobbies).intersection(set(s2_lobbies)))
                    if len(both_streamer_lobbies) == 0:
                        streamer_utc_diff[s1][s2] = None
                    else:
                        n = 0
                        abs_difference = timedelta(seconds=0)
                        for l in both_streamer_lobbies:
                            abs_difference += trustworthy_streamer_dic[s1][l][0] - trustworthy_streamer_dic[s2][l][0]
                            abs_difference += trustworthy_streamer_dic[s1][l][1] - trustworthy_streamer_dic[s2][l][1]
                            n += 2
                        streamer_utc_diff[s1][s2] = abs_difference / n
            """


            # print lobbies for which i do not have trustworthy times
            print("\n\n\n")
            print("LOBBIES WITHOUT TRUSTWORTHY TIMES:")
            lobbies_wo_trustworthy_times = []
            for i in lobby_dic.keys():
                if len(lobby_dic[i]['trustworthy_times']) == 0:
                    lobbies_wo_trustworthy_times.append(i)
                    print(f'Lobby has no trustworthy times: {i} - Start: {lobby_dic[i]["lobby_start"]} - End: {lobby_dic[i]["lobby_end"]}')

            # print streamers for which i do not have trustworthy times
            print("\n\n\n")
            for i in trustworthy_streamer_dic.keys():
                if len(trustworthy_streamer_dic[i]) == 0:
                    print(f'Streamer has no trustworthy times: {i} - Start time: {df[df["path"]==i]["start_date"]}')

            # print streamer who participated in all lobbies
            print("\n\n\n")
            print("STREAMERS WHO PARTICIPATED IN ALL LOBBIES:")
            df['lobbies_participated'] = df['lobbies_assigned_final'].apply(lambda lst: len(lst))
            top_streamer = list(df[df['lobbies_participated'] == max(df['lobbies_participated'])]['path'])
            print(f'Streamers who participated in all lobbies: {top_streamer}')


            # for streamers who participated in all lobbies print lobby times
            print("\n\n\n")
            print("LOBBY TIMES OF STREAMERS WHO PARTICIPATED IN ALL LOBBIES:")
            for s in top_streamer:
                times_critical = []
                lobbies_not_existing = 0
                times_all = []
                for l in lobbies_wo_trustworthy_times:
                   if l in list(df[df['path'] == s]['lobbies_assigned_final'])[0]:
                       ind = list(df[df['path'] == s]['lobbies_assigned_final'])[0].index(l)
                       times_critical.append([l, list(df[df['path'] == s]['lobbies_times_final'])[0][ind]])
                   else:
                       lobbies_not_existing += 1

                for l in list(lobby_dic.keys()):
                    if l in list(df[df['path'] == s]['lobbies_assigned_final'])[0]:
                        ind = list(df[df['path'] == s]['lobbies_assigned_final'])[0].index(l)
                        times_all.append([l, list(df[df['path'] == s]['lobbies_times_final'])[0][ind]])

                # change lobby times to srt format for better manual extraction
                start_date = list(df[df['path'] == s]['start_date'])[0]

                srt_times_critical = []
                for item in times_critical:
                    new_sublist = [item[0]]
                    timestamps = item[1]

                    if timestamps[0] is not None:
                        new_sublist.append(timestamps[0] - start_date)
                    else:
                        new_sublist.append(None)

                    if timestamps[1] is not None:
                        new_sublist.append(timestamps[1] - start_date)
                    else:
                        new_sublist.append(None)

                    srt_times_critical.append(new_sublist)


                srt_times_all = []
                for item in times_all:
                    new_sublist = [item[0]]
                    timestamps = item[1]

                    if timestamps[0] is not None:
                        new_sublist.append(timestamps[0] - start_date)
                    else:
                        new_sublist.append(None)

                    if timestamps[1] is not None:
                        new_sublist.append(timestamps[1] - start_date)
                    else:
                        new_sublist.append(None)

                    srt_times_all.append(new_sublist)

                print(f'Streamer: {s} -- Nr of critical lobbies not existing:{lobbies_not_existing} -- {srt_times_critical}')
                print(f'Streamer: {s} -- {srt_times_all}')

            # extract assigned lobby numbers from df as csv to insert manual lobby extraction results
            df_lobby_numbers = df[['path', 'lobbies_assigned_final']]
            df_lobby_numbers.to_csv(f'{session}.csv', index=False)

            # extract dictionary with trustworthy lobby times for streamers
            extract_dic = {}
            for i in trustworthy_streamer_dic.keys():
                extract_dic[i] = {}
                extract_dic[i]['start_time'] = df[df["path"] == i]["start_date"]
                extract_dic[i]['lobbies'] = trustworthy_streamer_dic[i]
                with open(f'{session}_streamer.pkl', 'wb') as f:
                    pickle.dump(extract_dic, f)


        except Exception as e:
            print(f"An error occurred: {e}")
