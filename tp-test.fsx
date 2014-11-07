#r @"bin/Inrush.dll"
open Inrush.Influx.Provider
open Inrush.Influx.Action

type db = Database<Server="http://localhost:8086", Database="bt", User="root", Password="root">
