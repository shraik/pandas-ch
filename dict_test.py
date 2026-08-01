gl_dict = {}


def mod_dict():
    print(gl_dict)
    gl_dict[1] = "123"


if __name__ == "__main__":
    gl_dict[2] = "222"
    mod_dict()
    print("main")
    print(gl_dict)
