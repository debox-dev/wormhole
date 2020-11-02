import math


class Vector3:
    def __init__(self, x, y, z):
        self.x, self.y, self.z = x, y, z

    @property
    def magnitude(self):
        return math.sqrt(self.x * self.x + self.y * self.y + self.z * self.z)

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y and self.z == other.z
