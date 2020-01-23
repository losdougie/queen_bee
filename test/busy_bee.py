import random
import sys
import time


def main():
    buzz_count = 25
    field = []
    for z in range(0, buzz_count):
        flora = random.choice(["_._", "___", "_*_", "_Q_", "_0_"])
        frames = [_ for _ in flora]
        field.extend(frames)
    sys.stdout.write("".join(field))
    chance = {
        ".": [0.2, 0.3],
        "*": [1, 0.6, 0.7, 0.8],
        "0": [0.3, 0.4],
        "Q": [0.7, 0.8, 0.9],
        "_": [0.1],
    }
    change = {".": ".", "0": "O", "*": "o", "Q": "O", "_": "_"}
    for buzz in range(0, len(field)):
        sys.stdout.write("\r")
        scene = []
        bzz = field[buzz]
        for frame in range(0, len(field)):
            if buzz == frame and bzz == "_":
                scene.append("`")
            else:
                scene.append(field[frame])
        field[buzz] = change[bzz]
        scene = "".join(scene)
        sys.stdout.write(scene)
        time.sleep(random.choice(chance[bzz]))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
