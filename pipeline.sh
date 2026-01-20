#!/bin/bash

docker cp ./mapping python:/app/
./pydoc.sh mapping/run_mapping.py

docker cp ./cleaning python:/app/
./pydoc.sh cleaning/cleaning.py

docker cp ./transformation python:/app/
./pydoc.sh transformation/transformation.py

docker cp ./analysis python:/app/
./pydoc.sh analysis/analysis.py

read -p "Press Enter to exit..."