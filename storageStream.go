package main

import "github.com/liip/sheriff"

//MarshalledStreamsList lists all streams and includes only fields which are safe to serialize.
func (obj *StorageST) MarshalledStreamsList() (interface{}, error) {
	obj.mutex.RLock()
	defer obj.mutex.RUnlock()
	val, err := sheriff.Marshal(&sheriff.Options{
		Groups: []string{"api"},
	}, obj.Streams)
	if err != nil {
		return nil, err
	}
	return val, nil
}

//StreamAdd add stream
func (obj *StorageST) StreamAdd(uuid string, val StreamST) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()
	//TODO create empty map bug save https://github.com/liip/sheriff empty not nil map[] != {} json
	//data, err := sheriff.Marshal(&sheriff.Options{
	//		Groups:     []string{"config"},
	//		ApiVersion: v2,
	//	}, obj)
	//Not Work map[] != {}
	if obj.Streams == nil {
		obj.Streams = make(map[string]StreamST)
	}
	if _, ok := obj.Streams[uuid]; ok {
		return ErrorStreamAlreadyExists
	}
	for i, i2 := range val.Channels {
		i2 = obj.StreamChannelMake(i2)
		if !i2.OnDemand {
			i2.runLock = true
			val.Channels[i] = i2
			go StreamServerRunStreamDo(uuid, i)
		} else {
			val.Channels[i] = i2
		}
	}
	obj.Streams[uuid] = val
	err := obj.SaveConfig()
	if err != nil {
		return err
	}
	return nil
}

//StreamEdit edit stream
func (obj *StorageST) StreamEdit(uuid string, val StreamST) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()
	if tmp, ok := obj.Streams[uuid]; ok {
		for i, i2 := range tmp.Channels {
			if i2.runLock {
				tmp.Channels[i] = i2
				obj.Streams[uuid] = tmp
				i2.signals <- SignalStreamStop
			}
		}
		for i3, i4 := range val.Channels {
			i4 = obj.StreamChannelMake(i4)
			if !i4.OnDemand {
				i4.runLock = true
				val.Channels[i3] = i4
				go StreamServerRunStreamDo(uuid, i3)
			} else {
				val.Channels[i3] = i4
			}
		}
		obj.Streams[uuid] = val
		err := obj.SaveConfig()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrorStreamNotFound
}

//StreamReload reload stream
func (obj *StorageST) StopAll() {
	obj.mutex.RLock()
	defer obj.mutex.RUnlock()
	for _, st := range obj.Streams {
		for _, i2 := range st.Channels {
			if i2.runLock {
				i2.signals <- SignalStreamStop
			}
		}
	}
}

//StreamReload reload stream
func (obj *StorageST) StreamReload(uuid string) error {
	obj.mutex.RLock()
	defer obj.mutex.RUnlock()
	if tmp, ok := obj.Streams[uuid]; ok {
		for _, i2 := range tmp.Channels {
			if i2.runLock {
				i2.signals <- SignalStreamRestart
			}
		}
		return nil
	}
	return ErrorStreamNotFound
}

//StreamDelete stream
func (obj *StorageST) StreamDelete(uuid string) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()
	if tmp, ok := obj.Streams[uuid]; ok {
		for _, i2 := range tmp.Channels {
			if i2.runLock {
				i2.signals <- SignalStreamStop
			}
		}
		delete(obj.Streams, uuid)
		err := obj.SaveConfig()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrorStreamNotFound
}

//StreamInfo return stream info
func (obj *StorageST) StreamInfo(uuid string) (*StreamST, error) {
	obj.mutex.RLock()
	defer obj.mutex.RUnlock()
	if tmp, ok := obj.Streams[uuid]; ok {
		return &tmp, nil
	}
	return nil, ErrorStreamNotFound
}

//StreamViewerCount counts all viewers across all channels of a stream
func (obj *StorageST) StreamViewerCount(uuid string) (int, error) {
	obj.mutex.RLock()
	defer obj.mutex.RUnlock()
	
	if tmp, ok := obj.Streams[uuid]; ok {
		totalViewers := 0
		for _, channel := range tmp.Channels {
			totalViewers += len(channel.clients)
		}
		return totalViewers, nil
	}
	return 0, ErrorStreamNotFound
}

//StreamChannelsAdd add multiple channels to existing stream
func (obj *StorageST) StreamChannelsAdd(uuid string, channels map[string]ChannelST, overwrite bool) (map[string]Message, error) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()
	
	if _, ok := obj.Streams[uuid]; !ok {
		return nil, ErrorStreamNotFound
	}

	if obj.Streams[uuid].Channels == nil {
		stream := obj.Streams[uuid]
		stream.Channels = make(map[string]ChannelST)
		obj.Streams[uuid] = stream
	}
	
	results := make(map[string]Message)
	addedCount := 0
	skippedCount := 0
	
	for channelID, channelConfig := range channels {
		if _, exists := obj.Streams[uuid].Channels[channelID]; exists && !overwrite {
			skippedCount++
			continue
		}
		
		if existingChannel, exists := obj.Streams[uuid].Channels[channelID]; exists && overwrite {
			if existingChannel.runLock {
				existingChannel.signals <- SignalStreamStop
			}
		}
		
		newChannel := obj.StreamChannelMake(channelConfig)
		stream := obj.Streams[uuid]
		stream.Channels[channelID] = newChannel
		obj.Streams[uuid] = stream
		
		if !newChannel.OnDemand {
			newChannel.runLock = true
			stream = obj.Streams[uuid]
			stream.Channels[channelID] = newChannel
			obj.Streams[uuid] = stream
			go StreamServerRunStreamDo(uuid, channelID)
		}
		
		addedCount++
		results[channelID] = Message{Status: 1, Payload: "Channel added successfully"}
	}
	
	if skippedCount > 0 {
		results["_summary"] = Message{
			Status: 1, 
			Payload: map[string]interface{}{
				"added":   addedCount,
				"skipped": skippedCount,
				"message": "Some channels were skipped because they already exist. Use overwrite=true to replace them.",
			},
		}
	}
	
	if addedCount > 0 {
		err := obj.SaveConfig()
		if err != nil {
			return results, err
		}
	}
	
	return results, nil
}
