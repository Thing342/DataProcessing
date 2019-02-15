from distutils.core import setup
from Cython.Build import cythonize

setup(
    ext_modules = cythonize("x_quadtree.pyx"), requires=['Cython']
)

