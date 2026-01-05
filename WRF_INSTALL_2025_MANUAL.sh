#!/bin/sh

: '
This shell script helps WRF users to install the Weather Research and Forecasting Model WRF version 4.5 and Ubuntu 24.04.1 LTS in a 64-bit system.

Author: Pradeep Kushwaha
***************************************************
PhD
CAOS IISC
'
#***********************************************************************************************************************************************

# Install required libraries including HDF5, NetCDF-C, NetCDF-Fortran, Jasper, Libpng, Zlib, and MPICH

sudo apt update
sudo apt upgrade -y
sudo apt install -y tcsh git libcurl4-openssl-dev
sudo apt install -y make gcc cpp gfortran openmpi-bin libopenmpi-dev
sudo apt install -y libtool automake autoconf make m4 default-jre default-jdk csh ksh git ncview build-essential unzip byacc flex bison

#*******************************************************************************

# Make required directories

export HOME=`cd; pwd`
mkdir -p $HOME/Models/WRF_TUTORIAL
export WRF_HOME=$HOME/Models/WRF_TUTORIAL
cd $WRF_HOME
mkdir -p Downloads Libs Libs/grib2 Libs/NETCDF Libs/MPICH
export DIR=$WRF_HOME/Libs
export CC=gcc
export CXX=g++
export FC=gfortran
export F77=gfortran
export FLEX_LIB_DIR=/usr/lib/x86_64-linux-gnu/

#***********************************************************************************************************************************************

# Download and install Zlib library

cd $WRF_HOME/Downloads
wget -c https://www.zlib.net/fossils/zlib-1.2.13.tar.gz
tar -xzvf zlib-1.2.13.tar.gz
cd zlib-1.2.13
./configure --prefix=$DIR/grib2
make 
make install

#***********************************************************************************************************************************************

# Download and install HDF5 library 

cd $WRF_HOME/Downloads
wget -c https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.13/hdf5-1.13.2/src/hdf5-1.13.2.tar.gz
tar -xvzf hdf5-1.13.2.tar.gz
cd hdf5-1.13.2
./configure --prefix=$DIR/grib2 --with-zlib=$DIR/grib2 --enable-hl --enable-fortran
make 
make install
export HDF5=$DIR/grib2
export LD_LIBRARY_PATH=$DIR/grib2/lib:$LD_LIBRARY_PATH

#***********************************************************************************************************************************************

# Download and install NetCDF-C library

cd $WRF_HOME/Downloads
wget -c https://github.com/Unidata/netcdf-c/archive/refs/tags/v4.9.0.tar.gz -O netcdf-c-4.9.0.tar.gz
tar -xzvf netcdf-c-4.9.0.tar.gz
cd netcdf-c-4.9.0
export CPPFLAGS=-I$DIR/grib2/include
export LDFLAGS=-L$DIR/grib2/lib
./configure --prefix=$DIR/NETCDF --disable-dap
make
make install
export PATH=$DIR/NETCDF/bin:$PATH
export NETCDF=$DIR/NETCDF

#***********************************************************************************************************************************************

# Download and install NetCDF-Fortran library

cd $WRF_HOME/Downloads
wget -c https://downloads.unidata.ucar.edu/netcdf-fortran/4.6.0/netcdf-fortran-4.6.0.tar.gz
tar -xvzf netcdf-fortran-4.6.0.tar.gz
cd netcdf-fortran-4.6.0
export LD_LIBRARY_PATH=$DIR/NETCDF/lib:$LD_LIBRARY_PATH
export CPPFLAGS=-I$DIR/NETCDF/include
export LDFLAGS=-L$DIR/NETCDF/lib
./configure --prefix=$DIR/NETCDF --disable-shared
make
make install

#***********************************************************************************************************************************************

# Download and install Jasper library

cd $WRF_HOME/Downloads
wget -c http://www.ece.uvic.ca/~mdadams/jasper/software/jasper-1.900.1.zip
unzip jasper-1.900.1.zip
cd jasper-1.900.1
autoreconf -i
./configure --prefix=$DIR/grib2
make
make install
export JASPERLIB=$DIR/grib2/lib
export JASPERINC=$DIR/grib2/include

#***********************************************************************************************************************************************

# Download and install Libpng library

cd $WRF_HOME/Downloads
wget -c https://sourceforge.net/projects/libpng/files/libpng16/1.6.39/libpng-1.6.39.tar.gz
tar -xzvf libpng-1.6.39.tar.gz
cd libpng-1.6.39/
export LDFLAGS=-L$DIR/grib2/lib
export CPPFLAGS=-I$DIR/grib2/include
./configure --prefix=$DIR/grib2
make
make install

#***********************************************************************************************************************************************

# Download and install MPICH library

cd $WRF_HOME/Downloads
wget -c https://www.mpich.org/static/downloads/4.0.3/mpich-4.0.3.tar.gz
tar -xzvf mpich-4.0.3.tar.gz
cd mpich-4.0.3
./configure --prefix=$DIR/MPICH --with-device=ch3 FFLAGS=-fallow-argument-mismatch FCFLAGS=-fallow-argument-mismatch
make
make install
export PATH=$DIR/MPICH/bin:$PATH

#***********************************************************************************************************************************************

# Download and install WRF library

cd $WRF_HOME/Downloads
wget -c https://github.com/wrf-model/WRF/releases/download/v4.5/v4.5.tar.gz -O wrf-4.5.tar.gz
tar -xzvf wrf-4.5.tar.gz -C $WRF_HOME
cd $WRF_HOME/WRFV4.5
ulimit -s unlimited
export WRF_EM_CORE=1
export WRF_NMM_CORE=0  
export WRFIO_NCD_LARGE_FILE_SUPPORT=1

./configure
# Select option 34 (dmpar GNU) for gfortran/gcc and option 1 (basic) for compile nesting

./compile em_real 2>&1 | tee wrf_compile.log
# Wait approximately 60 minutes to complete the installation

export WRF_DIR=$WRF_HOME/WRFV4.5

# Check for the existence of executable files

ls -lah main/*.exe
ls -lah run/*.exe
ls -lah test/em_real/*.exe

# #***********************************************************************************************************************************************

# # Compile the WRF-Chem external emissions conversion code

# ./compile emi_conv 2>&1 | tee emission_compile.log

# # Check for the existence of the executable file
# ls -lah test/em_real/*.exe

#***********************************************************************************************************************************************

# Download and install WPS library

cd $WRF_HOME/Downloads
wget -c https://github.com/wrf-model/WPS/archive/refs/tags/v4.5.tar.gz -O wps-4.5.tar.gz
tar -xzvf wps-4.5.tar.gz -C $WRF_HOME
cd $WRF_HOME/WPS-4.5

export JASPERLIB=$DIR/grib2/lib
export JASPERINC=$DIR/grib2/include

./configure
# Select option 3 (Linux x86-64) gfortran (dmpar) for gfortran and distributed memory
./compile
export PATH=$DIR/bin:$PATH
export LD_LIBRARY_PATH=$DIR/lib:$LD_LIBRARY_PATH

# Check for the existence of executable files
ls *.exe

#**********************************************************************************************************************************************