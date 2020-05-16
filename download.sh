#!/bin/sh
rm -rfv ~/buscador/data/*
hadoop fs -get /Documents ~/buscador/data/
hadoop fs -get /InvertedIndex ~/buscador/data/
hadoop fs -get /Frequency ~/buscador/data/
hadoop fs -get /Total ~/buscador/data/
