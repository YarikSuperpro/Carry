--!strict
--!optimize 2
--!native
--[[
Copyright 2026 Yarik_superpro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
		limitations under the License.
]]
local __WorkersReady = 0
local WorkersRequired = 1

local retry_retrive_limit = 4
local retry_retrive_delay = 1

local retry_establish_session_limit = 1
local retry_establish_session_delay = 2

local retry_close_session_limit = 3
local retry_close_session_delay = 7

local AutoSave_initial = 70
local AutoSave_delay = 60
local retry_AutoSave_limit = 1
local retry_AutoSave_delay = 5

local session_limit_seconds = 300
local StartingSize = 1024

local max_opcodes = 255


local JobId = game.JobId
local DataStoreService = game:GetService("DataStoreService")
local Players = game:GetService("Players")
local PlayerData = DataStoreService:GetDataStore("PlayerData")

local GetAsync = PlayerData.GetAsync
local SetAsync = PlayerData.SetAsync
local UpdateAsync = PlayerData.UpdateAsync

type WriteFunction = (plr:Player,buff:buffer,start:number)->(buffer,number)
type ReadFunction = (plr:Player,buff:buffer,start:number)->number
type PlrArgFunction = (plr:Player)->()

local WriteCallback = table.create(max_opcodes)::{[number]:WriteFunction}
local ReadCallback = table.create(max_opcodes)::{[number]:ReadFunction}
local DefaultCallback = table.create(max_opcodes)::{[number]:PlrArgFunction}
local CleanseCallback = table.create(max_opcodes)::{[number]:PlrArgFunction}

local UnexpectedCarry = {}::{[Player]:{[number]:string}}
local RuntimeDataAlive = {}::{[Player]:true}
local Sessions = {}::{[Player]:true}
local Sessions_count = 0
local Session_Starting = {}::{[Player]:thread}

local AutoUpdate_thread = {}::{[Player]:thread}

local GetPlayerDataBuffer = function(Plr:Player):buffer
	local buff = buffer.create(StartingSize)
	local pos = 0
	for i,v in WriteCallback do
		local n,p = v(Plr,buff,pos)
		buff=n
		pos+=p
	end
	local carry_size = 0
	local carry = UnexpectedCarry[Plr]
	for i,v in carry do carry_size+=#v end
	local bounds = pos+carry_size

	local new = buffer.create(bounds)
	buffer.copy(new,0,buff,0,pos)
	
	for i,v in carry do
		buffer.writestring(new,pos,v)
		pos+=#v
	end
	
	return new
end
local CleanseRuntimeData = function(Plr:Player):()
	RuntimeDataAlive[Plr]=nil
	UnexpectedCarry[Plr]=nil
	for i,v in CleanseCallback do
		v(Plr)
	end
end

local AutoUpdate:(plr:Player)->()
AutoUpdate = function(plr:Player):()
	AutoUpdate_thread[plr]=task.delay(AutoSave_delay,AutoUpdate,plr)
	local id = tostring(plr.UserId)
	local buff = GetPlayerDataBuffer(plr)
	local info = {
		JobId = JobId;
		Set = workspace:GetServerTimeNow();
	}
	local func = function(value:buffer,metadata:DataStoreKeyInfo?):any
		if not metadata then return buff,nil,info end
		local MetaInfo = metadata:GetMetadata()
		if MetaInfo["JobId"]~=JobId then
			plr:Kick("The session already exists, autosave canceled.")
			return
		end
		return buff,nil,info
	end
	
	local try = pcall(UpdateAsync,PlayerData,id,func)
	if not try then
		for i=1,retry_AutoSave_limit do
			task.wait(retry_AutoSave_delay)
			try = pcall(UpdateAsync,PlayerData,id,func)
		end
	end
end

type pcall_getasync = (typeof(GetAsync),DataStore,string)->(boolean,buffer?,DataStoreKeyInfo?)

local PlayerAdded_func = function(plr:Player):()
	if Sessions[plr] then plr:Kick("Session has not yet ended.") return end
	if RuntimeDataAlive[plr] then plr:Kick("Runtime data hasn't been removed yet.") return end
	if Session_Starting[plr] then plr:Kick("Session was starting already.") return end

	
	local id = tostring(plr.UserId)

	Session_Starting[plr]=coroutine.running()

	--Get data
	local try,get,metadata = (pcall::pcall_getasync)(GetAsync,PlayerData,id)

	if not try then
		for i=1,retry_retrive_limit do
			task.wait(retry_retrive_delay)
			try,get,metadata = (pcall::pcall_getasync)(GetAsync,PlayerData,id)
			if try then break end
		end
		if not try then
			--ran out of tries
			plr:Kick("Unable to load data.")
			Session_Starting[plr]=nil
			return
		end
	end

	local DefaultCallback = DefaultCallback
	local ReadCallback = ReadCallback

	if not get then--new user
		RuntimeDataAlive[plr]=true
		UnexpectedCarry[plr]=table.create(max_opcodes)::{[number]:string}
		for i,v in DefaultCallback do
			v(plr)
		end
		local buff = GetPlayerDataBuffer(plr)
		local info = {
			JobId = JobId;
			Set = workspace:GetServerTimeNow();
		}
		
		local func = function(value:buffer?,metadata:DataStoreKeyInfo):any
			if value or metadata then
				CleanseRuntimeData(plr)
				Session_Starting[plr]=nil
				plr:Kick("Other session already exists.")
				return
			end
			return buff,nil,info
		end
		local try = pcall(UpdateAsync,PlayerData,id,func)
		if not try then
			for i=1,retry_establish_session_limit do
				task.wait(retry_establish_session_delay)
				try = pcall(UpdateAsync,PlayerData,id,func)
			end
		end
		if not try then
			CleanseRuntimeData(plr)
			Session_Starting[plr]=nil
			plr:Kick("Unable to lock the session.")
			return
		end
	else--already played
		if metadata then
			local MetaInfo = metadata:GetMetadata()
			local j = MetaInfo["JobId"] or JobId
			local set_time = (MetaInfo["Set"] or 0)+session_limit_seconds
			local current_time = workspace:GetServerTimeNow()
			if JobId~=j and set_time>current_time then
				plr:Kick("Session has not been closed\n please wait:\n "..tostring(set_time-current_time).." seconds.")
				Session_Starting[plr]=nil
				return
			end
		end
		--valid metadata
		if type(get)~="buffer" then
			plr:Kick("Datatype is not buffer.")
			Session_Starting[plr]=nil
			return
		end
		--create runtime data
		local Plr_Carry = table.create(max_opcodes)::{[number]:string}
		UnexpectedCarry[plr]=Plr_Carry
		local ReadOpcodesList = table.clone(DefaultCallback)
		local pos = 0
		local len = buffer.len(get)
		RuntimeDataAlive[plr]=true

		while pos<len do
			local opcode = buffer.readu8(get,pos)
			local transformer = ReadCallback[opcode]
			if not transformer then--unregistered opcode
				--move to carry
				local mov = buffer.readu16(get,pos+1)+3
				Plr_Carry[opcode]=buffer.readstring(get,pos,mov)
				pos+=mov
				continue
			end
			local res = transformer(plr,get,pos)
			pos+=res
			ReadOpcodesList[opcode]=nil
		end
		for i,v in ReadOpcodesList do
			v(plr)
		end
		local buff = GetPlayerDataBuffer(plr)
		local info = {
			JobId = JobId;
			Set = workspace:GetServerTimeNow();
		}
		local func = function(value:buffer?,metadata:DataStoreKeyInfo):any
			if metadata then
				local MetaInfo = metadata:GetMetadata()
				local j = MetaInfo["JobId"] or JobId
				local set_time = (MetaInfo["Set"] or 0)+session_limit_seconds
				local current_time = workspace:GetServerTimeNow()
				if JobId~=j and set_time>current_time then
					plr:Kick("The loading has failed due to a new session getting opened.")
					CleanseRuntimeData(plr)
					Session_Starting[plr]=nil
					return
				end
			end
			return buff,nil,info
		end
		local try = pcall(UpdateAsync,PlayerData,id,func)
		if not try then
			for i=1,retry_establish_session_limit do
				task.wait(retry_establish_session_delay)
				try = pcall(UpdateAsync,PlayerData,id,func)
			end
		end
		if not try then
			CleanseRuntimeData(plr)
			Session_Starting[plr]=nil
			plr:Kick("Unable to lock the session.")
			return
		end
	end

	AutoUpdate_thread[plr]=task.delay(AutoSave_initial,AutoUpdate,plr)
	Session_Starting[plr]=nil
	Sessions[plr]=true
	Sessions_count+=1
end
local PlayerRemoving_func = function(plr:Player):()
	local starting = Session_Starting[plr]
	if starting then task.cancel(starting) end
	local save = AutoUpdate_thread[plr]
	if save then task.cancel(save) AutoUpdate_thread[plr]=nil end
	
	local id = tostring(plr.UserId)

	if RuntimeDataAlive[plr] then
		if Sessions[plr] then
			--save the new info and free metadata
			local buff = GetPlayerDataBuffer(plr)
			local func = function(value:buffer?,metadata:DataStoreKeyInfo):any
				if metadata then
					local MetaInfo = metadata:GetMetadata()
					local j = MetaInfo["JobId"] or JobId
					local set_time = (MetaInfo["Set"] or 0)+session_limit_seconds
					local current_time = workspace:GetServerTimeNow()
					if JobId~=j and set_time>current_time then
						return
					end
				end
				return buff
			end
			local try = pcall(UpdateAsync,PlayerData,id,func)
			if not try then
				for i=1,retry_close_session_limit do
					task.wait(retry_close_session_delay)
					try = pcall(UpdateAsync,PlayerData,id,func)
				end
			end
			
			Sessions[plr]=nil
			Sessions_count-=1
		end
		CleanseRuntimeData(plr)
		Session_Starting[plr]=nil
		return
	end
	if Sessions[plr] then
		--remove session lock metadata here and keep info as is
		local func = function(value:buffer?,metadata:DataStoreKeyInfo):any
			if metadata then
				local MetaInfo = metadata:GetMetadata()
				local j = MetaInfo["JobId"] or JobId
				local set_time = (MetaInfo["Set"] or 0)+session_limit_seconds
				local current_time = workspace:GetServerTimeNow()
				if JobId~=j and set_time>current_time then
					return
				end
			end
			return value
		end
		local try = pcall(UpdateAsync,PlayerData,id,func)
		if not try then
			for i=1,retry_close_session_limit do
				task.wait(retry_close_session_delay)
				try = pcall(UpdateAsync,PlayerData,id,func)
			end
		end
		Sessions[plr]=nil
		Sessions_count-=1
		Session_Starting[plr]=nil
		return
	end
end



local RegisterOpcode = function(opcode:number,write:WriteFunction,read:ReadFunction,def:PlrArgFunction,clear:PlrArgFunction):()
	WriteCallback[opcode]=write
	ReadCallback[opcode]=read
	DefaultCallback[opcode]=def
	CleanseCallback[opcode]=clear
end

--The opcode has to follow U16 lengh prefix convention after opcode part
local RegisterDynamicOpcode = function(opcode:number,write:WriteFunction,read:ReadFunction,def:PlrArgFunction,clear:PlrArgFunction):()
	WriteCallback[opcode]=write
	ReadCallback[opcode]=read
	DefaultCallback[opcode]=def
	CleanseCallback[opcode]=clear
	for i,v in UnexpectedCarry do
		local data = v[opcode]
		if data then
			v[opcode]=nil
			read(i,buffer.fromstring(data),0)
			continue
		end
		def(i)
	end
end

--The opcode has to follow U16 lengh prefix convention after opcode part
local UnloadDynamicOpcode = function(opcode:number):()
	local write = WriteCallback[opcode]
	local clear = CleanseCallback[opcode]
	WriteCallback[opcode]=nil
	ReadCallback[opcode]=nil
	DefaultCallback[opcode]=nil
	CleanseCallback[opcode]=nil
	
	local buff = buffer.create(StartingSize)
	for i in RuntimeDataAlive do
		local b,s=write(i,buff,0)
		local carry = UnexpectedCarry[i]
		carry[opcode]=buffer.readstring(b,0,s)
		clear(i)
	end
end



local WorkerInit = function():()
	local WorkersReady = __WorkersReady+1
	__WorkersReady=WorkersReady
	if WorkersReady~=WorkersRequired then return end
	--start
	local Players = Players
	local PlayerAdded = Players.PlayerAdded
	local GetPlayers = Players.GetPlayers
	local Player_Join_Connection = PlayerAdded:Connect(PlayerAdded_func)
	local Player_Leave_Connection = Players.PlayerRemoving:Connect(PlayerRemoving_func)

	for i,v in GetPlayers(Players) do
		task.spawn(PlayerAdded_func,v)
	end
	
	game:BindToClose(function():()
		Player_Join_Connection:Disconnect()
		Player_Leave_Connection:Disconnect()
		PlayerAdded:Connect(function(plr:Player):()
			plr:Kick("Server shutdown.")
		end)
		for i,v in AutoUpdate_thread do
			task.cancel(v)
		end
		for i,v in GetPlayers(Players) do
			task.spawn(PlayerRemoving_func,v)
		end
		while Sessions_count>0 do
			task.wait(1)
		end
	end)
end

export type WorkerInit = typeof(WorkerInit)
export type RegisterOpcode = typeof(RegisterOpcode)
export type RegisterDynamicOpcode = typeof(RegisterDynamicOpcode)
export type UnloadDynamicOpcode = typeof(UnloadDynamicOpcode)
export type RuntimeDataAlive = typeof(RuntimeDataAlive)
export type Sessions = typeof(Sessions)

return {
	[1] = WorkerInit;
	[2] = RegisterOpcode;
	[3] = RegisterDynamicOpcode;
	[4] = UnloadDynamicOpcode;
	[5] = RuntimeDataAlive;
	[6] = Sessions;
}
