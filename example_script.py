import json

from dejavu import Dejavu
from dejavu.config.settings import RESULTS, \
    SONG_ID, OFFSET, OFFSET_SECS, FIELD_FILE_SHA1, FINGERPRINTED_HASHES, \
    INPUT_CONFIDENCE, FINGERPRINTED_CONFIDENCE, INPUT_HASHES, HASHES_MATCHED
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
from dejavu.logic.recognizer.microphone_recognizer import MicrophoneRecognizer

# load config from a JSON file (or anything outputting a python dictionary)
with open("dejavu.cnf.SAMPLE") as f:
    config = json.load(f)


def without_keys(d, keys):
    return {x: d[x] for x in d if x not in keys}


if __name__ == '__main__':
    # create a Dejavu instance
    djv = Dejavu(config)

    # Fingerprint all the mp3's in the directory we give it
    djv.fingerprint_directory("test", [".wav", ".mp3"])

    # Recognize audio from a file
    results = djv.recognize(FileRecognizer, "mp3/obeme_korova.mp3")

    exclude_keys = [SONG_ID, OFFSET, OFFSET_SECS, FIELD_FILE_SHA1, FINGERPRINTED_HASHES]
    songs = sorted([without_keys(item, exclude_keys) for item in results[RESULTS]],
                   key=lambda song: (song[FINGERPRINTED_CONFIDENCE], song[INPUT_CONFIDENCE]),
                   reverse=True
                   )

    del results[RESULTS]
    print(f'Stats: {results}')
    print(f"From file we recognized: {json.dumps(songs, indent=4)}")
