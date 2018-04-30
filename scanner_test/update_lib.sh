# on amazon Ubuntu ami 

cp /lib/x86_64-linux-gnu/libc.so.6 .
#cp /usr/lib/x86_64-linux-gnu/libstdc++.so.6 . 
#error: /lib64/libc.so.6: version `GLIBC_2.18' not found --> use libstdc++.so.6 from amazon linux machine
cp /usr/lib/x86_64-linux-gnu/libboost_python-py27.so.1.58.0 .
#sudo apt-get install libboost-dev-all 
# - /usr/lib/x86_64-linux-gnu
# - /usr/include/boost

cp ~/cppcrail/build/client/libcppcrail.so .
cp ~/cppcrail/build/pocket/libpocket.so .
cp ~/crail-dispatcher/python-client/pocket.py .
