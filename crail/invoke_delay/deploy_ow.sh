rm -f ow_map.zip
rm -f __main__.py
cp ow_map.py __main__.py

zip -r ow_map.zip __main__.py crail.py bin conf jars


rm -f ow_reduce.zip
rm -f __main__.py
cp ow_reduce.py __main__.py

zip -r ow_reduce.zip __main__.py crail.py bin conf jars
rm -f __main__.py

