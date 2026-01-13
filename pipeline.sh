#!/bin/bash

docker cp ./mapping python:/app/
./pydoc.sh mapping/map.py

docker cp ./cleaning python:/app/
./pydoc.sh cleaning/cleaning.py

docker cp ./transformation python:/app/
./pydoc.sh transformation/transformation.py

read -p "Press Enter to exit..."