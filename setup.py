import fnmatch
from setuptools import find_packages, setup
from setuptools.command.build_py import build_py as build_py_orig

exclude = ['pipeline_runner.main']


class BuildPy(build_py_orig):

    def find_package_modules(self, package, package_dir):

        modules = super().find_package_modules(package, package_dir)
        return [(pkg, mod, file, ) for (pkg, mod, file, ) in modules
                if not any(fnmatch.fnmatchcase(pkg + '.' + mod, pat=pattern) for pattern in exclude)]


setup(

    name="pypelineRunner",
    version="0.0.1",
    author="Luca Carloni",
    author_email="carloni.luca91@gmail.com",
    url="https://github.com/carloxluca91/PipelineRunnerPython.git",
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_data={"pipeline_runner": ["logging.ini"]},
    include_package_data=True,
    cmdclass={'build_py': BuildPy},
    install_requires=["pyspark>=2.2", "numpy", "pandas", "mysql-connector-python"],
    python_requires='>=3.6'
)
