from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_subclip
from moviepy.editor import VideoFileClip
from pyannote.audio import Pipeline
import pickle


# speaker diarization
auth_token = "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"
auth_token_write = "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"


# extract 58:00 to 1:00:00 from ozzaworld video: at the beginning only him speaking, in the end discussion round
def extract_segment(input_file, output_file, start_time, end_time):
    # start_time and end_time are in seconds
    ffmpeg_extract_subclip(input_file, start_time, end_time, targetname=output_file)

# convert to audio wav file
def convert_mkv_to_wav(mkv_file, wav_file):
    video = VideoFileClip(mkv_file)
    audio = video.audio
    audio.write_audiofile(wav_file)


# test how good diarization performs
pipeline = Pipeline.from_pretrained('pyannote/speaker-diarization-3.1',
                                    use_auth_token='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')


# run code for one entire lobby: 2022-02-16_S1 L7
with open('../Lobby-Synchronization/data/final_synchronization_output', 'rb') as file:
    lobbies = pickle.load(file)

final_dic = {}

streamers = list(lobbies['2022-02-16_S1'][7].keys())

for s in streamers:
    start_time = lobbies['2022-02-16_S1'][7][s][0].seconds()
    end_time = lobbies['2022-02-16_S1'][7][s][1].seconds()
    extract_segment(f'../../../../dimstore/pop520978/data/2022-02-16_S1/{s}.mkv', f'{s}_l7.mkv', start_time, end_time)
    convert_mkv_to_wav(f'{s}_l7.mkv', f'{s}_audio.wav')

for s in streamers:
    pipeline = Pipeline.from_pretrained('pyannote/speaker-diarization-3.1',
                                        use_auth_token='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')

    diarization = pipeline(f"{s}_audio.wav")

    potential_discussion_starts = []
    second_last_speaker = "SPEAKER_01"
    second_last_speaker_start = 0
    last_speaker = "SPEAKER_01"
    last_speaker_start = 0
    for turn, _, speaker in diarization.itertracks(yield_label=True):
        current_speaker = speaker
        current_speaker_start = turn.start
        if current_speaker == second_last_speaker or current_speaker == last_speaker or last_speaker == second_last_speaker:
            second_last_speaker = last_speaker
            second_last_speaker_start = last_speaker_start
            last_speaker = current_speaker
            last_speaker_start = current_speaker_start
            continue
        else:
            if abs(current_speaker_start - second_last_speaker_start) < 10:
                potential_discussion_starts.append([second_last_speaker_start, turn.end])
            second_last_speaker = last_speaker
            second_last_speaker_start = last_speaker_start
            last_speaker = current_speaker
            last_speaker_start = current_speaker_start

    # now go through created list of potential discussion starts and extract the timestamps lying close to each other
    extracted_discussion_rounds = {}
    new_start = potential_discussion_starts[0][0]
    round_n = 1
    for ind, t in enumerate(potential_discussion_starts):
        if ind == 0:
            continue
        if t[0] - potential_discussion_starts[ind - 1][0] < 60:
            if t[1] < potential_discussion_starts[ind - 1][1]:
                potential_discussion_starts[ind][1] = potential_discussion_starts[ind - 1][1]
            if ind != len(potential_discussion_starts) - 1:
                continue
            else:  # last potential discussion start, thus register as last potential discussion in this lobby
                extracted_discussion_rounds[round_n] = [new_start, t[1]]
        else:
            extracted_discussion_rounds[round_n] = [new_start, potential_discussion_starts[ind - 1][1]]
            new_start = t[0]
            round_n += 1
            if ind == len(potential_discussion_starts) - 1:
                extracted_discussion_rounds[round_n] = [new_start, t[1]]

    final_dic[s] = extracted_discussion_rounds



