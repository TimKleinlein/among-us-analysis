import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import os
import pysrt
from pysrt import SubRipTime
import sys
import pickle

# extract the different sessions of the streaming data
sessions = os.listdir("../../../../../../dimstore/pop520978/data")
sessions.remove('unsorted')
sessions.remove('copy_files.sh')

# go over all sessions individually and create first lobby assignments for the streamers using the metadata and srt files of the VOD streams
for session in sessions:
    # create an output file for each session to store the results of the programmatic lobby assignments to later improve these results by a manual lobby inspection
    with open(f'../../data/initial_synchronization_output/Extraction/{session}.txt', 'w') as file:
        sys.stdout = file

        print(f"SESSION: {session}")
        print("\n\n\n")

        try:
            # create a delete streamer list to possibly delete streamers from the lobby synchronization process if image extraction did not work at all for their VOD stream
            delete_streamer = True
            delete_streamer_list = []
            while delete_streamer:

                # create a dataframe storing the relevant metadata of the VOD for each streamer of the session
                con = sqlite3.connect("../../data/streams_metadata/vods.db")
                cur = con.cursor()
                g = session
                res = cur.execute("SELECT v.id, m.path, v.published_at, v.start, v.end, m.duration "
                                  "FROM metadata m  JOIN vods v ON m.vod_id = v.id  "
                                  "WHERE `group` = ?", (g,))
                data = res.fetchall()
                for name in delete_streamer_list:
                    data = [tup for tup in data if tup[1][39:] != f"{name}.mkv"]

                columns = ['id', 'path', 'published_at', 'start_delta', 'end_delta', 'duration']
                df = pd.DataFrame(data, columns=columns)

                # Preprocess DataFrame: calculate the correct start and end date for each streamer based on their publish time, start delta and duration
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

                # EXTRACT EVENT DATA FROM SRT FILES: for each streamer i have srt file storing each extracted event from image recognition:
                # extracted events are either lobby start or lobby ends, have a start timestamp, an end timestamp and a duration

                srt_path = f'../../../../../../dimstore/pop520978/data/{session}/srt'
                # remove streamers for which lobby assignment did not work at all
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

                # store first lobby proposals for each streamer in dictionary streamer_lobbies
                # one lobby proposal exists of a lobby start and a lobby end. Create these lobby proposals such that whenever the order
                # of lobby start -> lobby end -> lobby start -> lobby end ... is broken the missing end or start is substituted by a None value
                streamer_lobbies = {}
                for streamer in streamers:
                    subs = pysrt.open(f"{srt_path}/{streamer}")
                    timestamps = []

                    for sub in subs:
                        # skip srt events with a duration not realistic for a lobby start / end event (the duration for a correctly recognized event is usually 6 seconds)
                        if sub.duration > SubRipTime(seconds=30) or sub.duration < SubRipTime(seconds=2):
                            continue
                        # convert srt events in time deltas as later timestamps are used to connect different streams
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


                # Apply the function to create a new column 'lobbies_times' which stores proposed lobbies as timestamps
                df['lobbies_times'] = df.apply(sum_timedeltas, axis=1)

                # kick all lobbies times with a duration < 1 minute (assumption that lobbies usually go longer and no problem if data of short lobby is lost)
                df['lobbies_times'] = df['lobbies_times'].apply(
                    lambda lst: [x for x in lst if x[1] is None or x[0] is None or (x[1] - x[0]) > timedelta(seconds=60)])

                # CREATE A DICTIONARY OF LOBBIES TO FIND LOBBIES OF THE SESSION USING DIFFERENT PROPOSED LOBBY TIMES OF STREAMERS
                # create list of all the lobbies times and sort them according to its time
                df_exploded = df.explode('lobbies_times')
                all_lobbies_list = df_exploded['lobbies_times'].tolist()
                sorted_all_lobbies_list = sorted(all_lobbies_list, key=lambda x: (x[0] is None, x[0]))

                # assign lobby times to a lobby dictionary: go over all time-sorted lobby times and either assign them to an existing session lobby
                # or create a new session lobby according to the lobby times data
                lobby_dic = {}
                num_counter = 1
                for lob in sorted_all_lobbies_list:
                    if lob[0] is None or lob[1] is None:  # lobbies containing None will be assigned later on
                        continue
                    assigned_to_lobby = False
                    for i in lobby_dic.keys():
                        # if there already is a session lobby which has a start date less than two minutes different from lobby time assign this lobby
                        # time to the existing session lobby
                        if lob[0] - lobby_dic[i]['lobby_start'] < timedelta(
                                minutes=2):  # and lob[1] - lobby_dic[i]['lobby_end'] < timedelta(minutes=2):
                            lobby_dic[i]['timestamp_list'].append(lob)
                            lobby_dic[i]['lobby_start'] = lob[0]
                            lobby_dic[i]['lobby_end'] = lob[1]
                            assigned_to_lobby = True
                            break
                    if assigned_to_lobby:
                        continue
                    # if no session lobby has a start date less than 2 minutes away from lobby time, this lobby time creates a new session lobby
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

                # now assign lobby times with None to lobbies: use the not None part of the lobby time and find session lobby which
                # has a median start / end not further away than two minutes
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
                # if that is the case, add streamer to delete list
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

            # merge lobbies according to end timestamps: if median lobby end of tow lobbies is less than 45 seconds apart, merge these two lobbies
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

            # EVALUATION OF LOBBY ASSIGNMENTS
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
            # check for all final lobbies for outliers within the lobbies and print them
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

            # check for all streamers if final lobbies assigned are subsequent numbers: if not print the exceptions in session log file
            print("THE FOLLOWING STREAMERS HAVE NON-CONSECUTIVE LOBBIES:")
            for j in range(len(df)):
                lobbies = df['lobbies_assigned_final'][j]
                for i in range(len(lobbies) - 1):
                    if lobbies[i] is None:
                        print(f'Streamer: {df["path"][j]}: Lobbies numbers not consecutive: {lobbies[i]} and {lobbies[i+1]}')
                    elif lobbies[i] + 1 != lobbies[i + 1]:
                        print(f'Streamer: {df["path"][j]}: Lobbies numbers not consecutive: {lobbies[i]} and {lobbies[i+1]}')

            # PREPARE NEXT STEPS USING THE RESULTS FROM THE PROGRAMMATIC LOBBY ASSIGNMENTS
            # for all lobbies find trustworthy lobby times: lobby times are trustworthy if at least one other lobby time in the same lobby
            # has a similar start date and end date (max 10 sec difference) and duration (max 5 seconds)
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


            # for all streamers find their trustworthy lobby times: go over all their lobby times and compare with the just created dictionary of trustworthy lobby times
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

            # print lobbies for which i do not have trustworthy times in log file of session
            print("\n\n\n")
            print("LOBBIES WITHOUT TRUSTWORTHY TIMES:")
            lobbies_wo_trustworthy_times = []
            for i in lobby_dic.keys():
                if len(lobby_dic[i]['trustworthy_times']) == 0:
                    lobbies_wo_trustworthy_times.append(i)
                    print(f'Lobby has no trustworthy times: {i} - Start: {lobby_dic[i]["lobby_start"]} - End: {lobby_dic[i]["lobby_end"]}')

            # print streamers for which i do not have trustworthy times in log file of session
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

            # for streamers who participated in all lobbies print lobby times for faster manual lobby extraction
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

            # extract assigned lobby numbers from df as csv to insert manual lobby extraction results in participated_lobbies.py
            df_lobby_numbers = df[['path', 'lobbies_assigned_final']]
            df_lobby_numbers.to_csv(f'../../data/initial_synchronization_output/assignedLobbiesDfs/{session}.csv', index=False)

            # extract dictionary with trustworthy lobby times for streamers to be used in streamer_dictionaries.py
            extract_dic = {}
            for i in trustworthy_streamer_dic.keys():
                extract_dic[i] = {}
                extract_dic[i]['start_time'] = df[df["path"] == i]["start_date"]
                extract_dic[i]['lobbies'] = trustworthy_streamer_dic[i]
            with open(
                    f'../../data/initial_synchronization_output/streamer_dictionaries/{session}_streamer.pkl', 'wb') as f:
                pickle.dump(extract_dic, f)

            # extract dictionary with trustworthy lobby times for lobbies to be used in lobbies_dictionaries.py
            extract_lobby_dic = {}
            for k in lobby_dic.keys():
                lobby = lobby_dic[k]
                extract_lobby_dic[k] = {}
                for times in lobby['trustworthy_times']:
                    for i in range(len(df)):
                        if times in df['lobbies_times_final'][i]:
                            extract_lobby_dic[k][df['path'][i]] = times
            with open(
                    f'../../data/initial_synchronization_output/lobbies_dictionaries/{session}_lobbies.pkl', 'wb') as f:
                pickle.dump(extract_lobby_dic, f)


        except Exception as e:
            print(f"An error occurred: {e}")
