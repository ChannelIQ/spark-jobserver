#!/bin/bash

if [ -d "env" ]; then 
    echo "Virtualenv already exists"
else
    echo "Creating virtualenv..."
    virtualenv env
fi

source env/bin/activate

pip install fabric