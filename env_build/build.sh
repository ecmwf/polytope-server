
#!/usr/bin/env bash
set -euo pipefail

# NOTE: YOU NEED TO HAVE uv installed.
# You may also need apt install python-dev and python-venv
# also need ninja.

# Creates a new python environment and installs pyfdb/pygribjump into it.
# Modify the script if you want it to do somethinf different...

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SRC_BUNDLE="${SRC_BUNDLE:-${SCRIPT_DIR}/git}"
BUILD_DIR="${BUILD_DIR:-${SCRIPT_DIR}/build}"
INSTALL_PREFIX="${INSTALL_PREFIX:-${SCRIPT_DIR}/install}"

# rm -rf "${BUILD_DIR}"
rm -rf "${INSTALL_PREFIX}/.venv"

# Build and install AEC v1.1.4 from source.
if [ ! -d "${SRC_BUNDLE}/libaec" ]; then
  git clone "https://gitlab.dkrz.de/k202009/libaec.git" "${SRC_BUNDLE}/libaec"
fi
cd "${SRC_BUNDLE}/libaec"
git checkout v1.1.4
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" -G "Ninja"
cmake --build build
cmake --install build --prefix "${INSTALL_PREFIX}"

# Clone ecbuild, add to path.
if [ ! -d "${SRC_BUNDLE}/ecbuild" ]; then
  git clone https://github.com/ecmwf/ecbuild.git "${SRC_BUNDLE}/ecbuild"
fi
export PATH="${SRC_BUNDLE}/ecbuild/bin:$PATH"


# Python deps...
# Create a python environment
if [ ! -d "${INSTALL_PREFIX}/.venv" ]; then
  # ${PYTHON_BIN} -m venv "${INSTALL_PREFIX}/.venv"
  uv venv "${INSTALL_PREFIX}/.venv" --python "3.12"
fi
source "${INSTALL_PREFIX}/.venv/bin/activate"

python -m ensurepip --upgrade
python -m pip install -U pip
uv pip install pybind11 build

PYBIND11_CMAKE_DIR="$(python -m pybind11 --cmakedir)"

# make sure it isnt empty
if [ -z "${PYBIND11_CMAKE_DIR}" ]; then
  echo "Error: PYBIND11_CMAKE_DIR is empty. Please check your pybind11 installation."
  exit 1
fi

# Build build build...
cp $SCRIPT_DIR/CMakeLists.txt.in "${SRC_BUNDLE}/CMakeLists.txt"
# Python_INCLUDE_DIRS Python_LIBRARIES
ecbuild -G "Ninja" -B ${BUILD_DIR} \
  -DPython_ROOT_DIR="${INSTALL_PREFIX}/.venv" \
  -DPython_EXECUTABLE="${INSTALL_PREFIX}/.venv/bin/python" \
  -DENABLE_FORTRAN=OFF \
  -DENABLE_MEMFS=ON \
  -DENABLE_AEC=ON \
  -DENABLE_PYTHON_FDB_INTERFACE=ON \
  -DCMAKE_PREFIX_PATH="${INSTALL_PREFIX};${PYBIND11_CMAKE_DIR}" \
  -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
  "${SRC_BUNDLE}"

cd ${BUILD_DIR} && ninja install

# install the pyfdb wheel we just build
uv pip install ${BUILD_DIR}/pyfdb*.whl

# install pygribjump from source
uv pip install ${SRC_BUNDLE}/gribjump/

export GRIBJUMP_DIR=${INSTALL_PREFIX} # use this one.
export FDB5_DIR=${INSTALL_PREFIX} # use this one

echo "Testing pyfdb and pygribjump imports"

python -c "import pyfdb; print('pyfdb import OK')"
python -c "import pygribjump; print('pygribjump import OK')"

profile=$SCRIPT_DIR/profile
echo "source ${INSTALL_PREFIX}/.venv/bin/activate" > $profile
echo "export PATH=${INSTALL_PREFIX}/bin:\$PATH" >> $profile
echo "export FINDLIBS_DISABLE_PACKAGE=yes # dont use system installed fdb/gribjump..." >> $profile
echo "export FDB5_DIR=${INSTALL_PREFIX} # use this one" >> $profile
echo "export GRIBJUMP_DIR=${INSTALL_PREFIX} # use this one." >> $profile
echo "export ECCODES_DIR=${INSTALL_PREFIX} # use this one." >> $profile

echo "To use, 'source $profile', or add to your environment some other way."