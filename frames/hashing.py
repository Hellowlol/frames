import binascii
import numpy as np

from video import video_frame_by_frame


image_type = ('.png', '.jpeg', '.jpg')


def _binary_array_to_hex(arr):
    return binascii.hexlify(arr.flatten()).decode('ascii')


class ImageHash(object):
    """
    Hash encapsulation. Can be used for dictionary keys and comparisons.
    """

    def __init__(self, binary_array):
        self.hash = binary_array.flatten()
        self.pos = []

    def add_pos(self, pos):
        self.pos.append(pos)

    def __str__(self):
        return _binary_array_to_hex(self.hash)

    def __repr__(self):
        return repr(self.hash)

    def __sub__(self, other):
        if other is None:
            raise TypeError('Other hash must not be None.')
        if self.hash.size != other.hash.size:
            raise TypeError('ImageHashes must be of the same shape.', self.hash.shape, other.hash.shape)
        return np.count_nonzero(self.hash != other.hash)

    def __eq__(self, other):
        if other is None:
            return False
        return np.array_equal(self.hash, other.hash)

    def __ne__(self, other):
        if other is None:
            return False
        return not np.array_equal(self.hash, other.hash)

    def __hash__(self):
        return sum([2 ** i for i, v in enumerate(self.hash) if v])

    def __iter__(self):
        yield self

    @property
    def size(self):
        return len(self.pos)

    def reshape(self, *args):
        # for lazy compat
        return self.hash.reshape(*args)



def create_imghash(img):
    """Create a phash"""
    import cv2

    if isinstance(img, str):
        img = cv2.imread(img, 0)

    return cv2.img_hash.pHash(img)



def hash_file(path, step=1, frame_range=False, end=None):
    # dont think this is need. Lets keep it for now.
    if isinstance(path, str) and path.endswith(image_type):
        yield ImageHash(create_imghash(path)), cv2.imread(path, 0), 0
        return

    for (h, pos) in video_frame_by_frame(path, frame_range=frame_range, step=step, end=end):
        hashed_img = create_imghash(h)
        nn = ImageHash(hashed_img)
        yield nn, h, pos