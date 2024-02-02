import pickle
import random
with open(f'Lobby-Synchronization/data/final_synchronization_output/final_lobby_times.pkl', 'rb') as f:
    final_lobby_times = pickle.load(f)


random.seed(9)
check_manually = []
for i in range(10):
    sessions = len(final_lobby_times.keys())
    rand_session = list(final_lobby_times.keys())[random.randint(0, sessions - 1)]
    dic = final_lobby_times[rand_session]

    lobbies = len(dic.keys())
    rand_lobby = list(dic.keys())[random.randint(0, lobbies - 1)]
    dic = dic[rand_lobby]

    streamer = len(dic.keys())
    rand_streamer = list(dic.keys())[random.randint(0, streamer - 1)]

    check_manually.append(f'{rand_session}-{rand_lobby}-{rand_streamer}')


print(check_manually)
