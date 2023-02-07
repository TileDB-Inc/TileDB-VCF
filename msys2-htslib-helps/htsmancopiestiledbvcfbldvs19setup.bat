rem execute from a visual studio 19 command prompt, with drv:\htslibwork populated with indicated items

echo on
set htslibwork=e:\htslibwork
rem htslibwork contains:
rem addpathsbldvs19.bat
rem htsmancopiestiledbvcfbldvs19setup.bat
rem m2w64-htslib.tar.bz2.zip

rem set htsblddrv=e:
set htsblddrv=%CD:~0,2%
echo htsblddrv is %htsblddrv%
rem working location to fetch github, create binary build path, stage etc.
set htsblddir=%htsblddrv%\htsbld

rem cd /d e:\dev\tdbvcf
rd /s /q %htsblddir%
mkdir %htsblddir%
cd %htsblddir%
git clone https://github.com/TileDB-Inc/TileDB-VCF.git ./TileDB-VCF.git
set tdbvcfsrc=%htsblddir%\TileDB-VCF.git
cd %tdbvcfsrc%
git checkout dlh/sc24772-msys2-msvc-htslib-feedstock-WIP1
cd %htsblddir%
mkdir tdbvcf
cd tdbvcf
rem ***CHANGEME!!*** if needed, path to addpaths...bat
copy %htslibwork%\addpathsbldvs19.bat .

rem obtain the msys2 built htslib from somewhere, this artifact seems to be gone...?
rem curl -O -L https://github.com/jdblischak/htslib-test-builds/suites/10770122402/artifacts/540990432
rem set htslib_source_dir=e:\dev\tdbvcf\blischak_htslib\Library
rem ***THIS WILL ALMOST CERTAINLY NEED CHANGE!!! 
rem ***CHANGEME!!!***
copy e:\htslibwork\m2w64-htslib.tar.bz2.zip .
mkdir htslib_expanded
cd htslib_expanded
cmake -E tar xvf ../m2w64-htslib.tar.bz2.zip
cmake -E tar xvf ./m2w64-htslib-1.15.1-0.tar.bz2
set htslib_source_dir=%htsblddir%\tdbvcf\htslib_expanded\Library

cd %htsblddir%\tdbvcf
rd /s /q bldvs19
mkdir bldvs19
cd bldvs19                                                                                                         
mkdir externals\install                                                                                            
mkdir externals\install\bin                                                                                        
mkdir externals\install\lib                                                                                       
mkdir externals\install\lib\htslib                                                                                
mkdir externals\install\include\htslib                                                                            
mkdir externals\install\lib\pkgconfig       

rem set htslib_source_dir=e:\new_msys64\htslib-1.15.1
rem need one appropriately patched for msvc usage...
rem set htslib_source_dir=e:\new_msys64\htslib-1.15.1.mingw64
rem hide.htslib-1.15.1.hide has the needed patches for tiledbvcf in place
rem set htslib_source_dir=e:\new_msys64\hide.htslib-1.15.1.hide 
rem set htslib_source_dir=e:\new_msys64\htslib-1.16
rem ... done further above
rem set htslib_source_dir=e:\dev\tdbvcf\blischak_htslib\Library

copy %htslib_source_dir%\bin\hts-3.dll .\externals\install\bin                                                  
copy %htslib_source_dir%\include\htslib\hts_defs.h .\externals\install\include\htslib                               
copy %htslib_source_dir%\include\htslib\hts_endian.h .\externals\install\include\htslib                             
copy %htslib_source_dir%\include\htslib\hts_expr.h .\externals\install\include\htslib                               
copy %htslib_source_dir%\include\htslib\hts_log.h .\externals\install\include\htslib                                
copy %htslib_source_dir%\include\htslib\hts.h .\externals\install\include\htslib                                    
copy %htslib_source_dir%\include\htslib\hts_os.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\include\htslib\vcf.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\include\htslib\vcfutils.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\include\htslib\hfile.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\include\htslib\kstring.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\include\htslib\k*.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\include\htslib\synced_bcf_reader.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\include\htslib\tbx.h .\externals\install\include\htslib                                 
copy %htslib_source_dir%\lib\hts-3.lib .\externals\install\lib                   

cd externals\install\include
rem ***CHANGEME!!!*** path to patch
rem c:\msys64\usr\bin\patch -p1 < e:\dev\tiledb\gh.tdb-vcf.2023.01.20.sc-24772.htslib.msvc.git\libtiledbvcf\cmake\patches\htslib.1.15.1.hts_defs.h.patch
rem c:\msys64\usr\bin\patch -p1 < e:\dev\tiledb\gh.tdb-vcf.2023.01.20.sc-24772.htslib.msvc.git\libtiledbvcf\cmake\patches\htslib.1.15.1.vcf.h.patch
c:\msys64\usr\bin\patch -p1 < %tdbvcfsrc%\libtiledbvcf\cmake\patches\htslib.1.15.1.hts_defs.h.patch
c:\msys64\usr\bin\patch -p1 < %tdbvcfsrc%\libtiledbvcf\cmake\patches\htslib.1.15.1.vcf.h.patch
cd ../../..
echo current dir is %cd%

rem cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo e:\dev\tiledb\gh.tdb-vcf.2023.01.20.sc-24772.htslib.msvc.git\libtiledbvcf 2>&1 | c:\msys64\usr\bin\tee cmake.out.1a.txt
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo %tdbvcfsrc%\libtiledbvcf 2>&1 | c:\msys64\usr\bin\tee cmake.out.1a.txt
cmake --build . --config RelWithDebInfo 2>&1 | c:\msys64\usr\bin\tee bld.out.1a.txt
dir tiledbvcf.exe /s
cmd /K "prompt sub-$P$G && call ..\addpathsbldvs19.bat && libtiledbvcf\src\RelWithDebInfo\tiledbvcf.exe"
