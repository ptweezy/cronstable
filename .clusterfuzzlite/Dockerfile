# Builds the fuzz targets in fuzz/ for ClusterFuzzLite; see fuzz/README.md.
# The repository root is the build context, so .dockerignore applies.
FROM gcr.io/oss-fuzz-base/base-builder-python:v1@sha256:21f6fab17ace4dc76e1c56de91ee95d3d03da021df94b41056ba036f6c6f55e9
COPY . $SRC/cronstable
WORKDIR $SRC/cronstable
COPY .clusterfuzzlite/build.sh $SRC/
