package main

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"net/http"

	"github.com/matryer/runner"
	flag "github.com/spf13/pflag"
)

// HDHRLineUp The structure returned from the HDHomeRun Prime
type HDHRLineUp struct {
	GuideNumber string `json:"GuideNumber"`
	GuideName   string `json:"GuideName"`
	VideoCodec  string `json:"VideoCodec"`
	AudioCodec  string `json:"AudioCodec"`
	URL         string `json:"URL"`
	HD          int    `json:"HD,omitempty"`
}

// NetworkEncoder is the actual network encoding instance
type NetworkEncoder struct {
	name   string
	port   int
	env    *EncoderEnvironments
	lineup map[int]string
}

// EncoderEnvironments is the actual global configuration and state, shared by all processes that need it
type EncoderEnvironments struct {
	globalEnvironment *GlobalEnvironmentVariables
	tunerNamesToTasks map[string]*runner.Task
	tunerPathsToSize  map[string]int
}

// GlobalEnvironmentVariables is Configurable at launch time global variables
type GlobalEnvironmentVariables struct {
	versionMajor int
	versionMinor int
	versionDev   int
	pathSearch   string
	pathReplace  string
	hdhrIP       string
	encoderID    string
	numEncoders  int
	baseName     string
	startPort    int
}

func readLine(conn net.Conn) (string, error) {
	buffer := make([]byte, 8192)
	data := ""
	var bubbledError error
	for !strings.Contains(data, "\r\n") && !strings.Contains(data, "\n") {
		n, err := conn.Read(buffer)
		if err != nil {
			bubbledError = err
			break
		}
		if n > 0 {
			data = data + string(buffer[:n])
		} else {
			break
		}
	}
	return data, bubbledError
}

func sendAll(conn net.Conn, s string) {
	data := []byte(string(s))
	n, err := conn.Write(data)
	if n < len(data) {
		log.Println("Short send, expected:", len(data), "sent:", n)
	}
	if err != nil {
		log.Println("Error on sending:", err)
	}
}

func (ne *NetworkEncoder) properties() []string {
	blob := make([]string, 0)
	blob = append(blob,
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/available_channels=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/brightness=-1",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/broadcast_standard=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/contrast=-1",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/device_name=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/hue=-1",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/last_channel=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/saturation=-1",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/sharpness=-1",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/tuning_mode=Cable",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/tuning_plugin=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/tuning_plugin_port=0",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/video_crossbar_index=0",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/1/0/video_crossbar_type=1",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/audio_capture_device_name=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/broadcast_standard=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/capture_config=2050",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/default_device_quality=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/delay_to_wait_after_tuning=0",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/encoder_merit=0",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/fast_network_encoder_switch=false",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/forced_video_storage_path_prefix",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/last_cross_index=0",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/last_cross_type=1",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/live_audio_input=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/multicast_host=",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/never_stop_encoding=false",
		string("mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/video_capture_device_name="+ne.name),
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/video_capture_device_num=0",
		"mmc/encoders/"+(*ne.env.globalEnvironment).encoderID+"/video_encoding_params=Great")
	data := make([]string, 0)
	data = append(data, strconv.Itoa(len(blob)))
	data = append(data, blob...)
	data = append(data, ne.ok()...)
	return data
}

func (ne *NetworkEncoder) ok() []string {
	return []string{"OK"}
}

func (ne *NetworkEncoder) fail() []string {
	return []string{"ERROR"}
}

func getParams(regEx, txt string) map[string]string {
	var compRegEx = regexp.MustCompile(regEx)
	match := compRegEx.FindStringSubmatch(txt)

	paramsMap := make(map[string]string)
	for i, name := range compRegEx.SubexpNames() {
		if i > 0 && i <= len(match) {
			paramsMap[name] = match[i]
		}
	}
	return paramsMap
}

func (ne *NetworkEncoder) start(command string) []string {
	params := getParams("START (?P<TunerName>[^|]+)\\|(?P<Nonce>\\d+)\\|(?P<Channel>\\d+)\\|(?P<Sigil>\\d+)\\|(?P<Path>[^|]+)\\|(?P<Quality>[^|]+).*", strings.Trim(command, " "))
	log.Println(params)
	channel, err := strconv.Atoi(params["Channel"])
	if err != nil {
		return ne.fail()
	}
	ne.env.tunerNamesToTasks[params["TunerName"]] = ne.openChannel(channel, params["Path"])
	return ne.ok()
}

func (ne *NetworkEncoder) getNewPath(path string) string {
	return strings.Replace(path, (*ne.env.globalEnvironment).pathSearch, (*ne.env.globalEnvironment).pathReplace, 1)
}

func (ne *NetworkEncoder) openChannel(channel int, path string) *runner.Task {
	log.Printf("Opening channel %d on %+v to %s (%s)\n", channel, ne, path, ne.getNewPath(path))
	if ne.lineup == nil {
		log.Println("Lineup isn't known, fetching (and caching)...")
		res, err := http.Get("http://" + (*ne.env.globalEnvironment).hdhrIP + "/lineup.json")
		if err != nil {
			log.Fatal(err)
			return nil
		}
		lineup, err := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			log.Fatal(err)
			return nil
		}
		//log.Printf("%s\n", lineup)
		keys := make([]HDHRLineUp, 0)
		json.Unmarshal(lineup, &keys)
		ne.lineup = make(map[int]string)
		for _, channel := range keys {
			if chanNum, err := strconv.Atoi(channel.GuideNumber); err == nil {
				ne.lineup[chanNum] = channel.URL
			}
		}
		log.Println("Added", len(ne.lineup), "number of channels.")
	}
	streamURL := ne.lineup[channel]
	log.Println("Queuing > Fetching channel", channel, "which is", streamURL)
	return runner.Go(func(shouldStop runner.S) error {
		// do setup
		log.Println("Executing > Fetching channel", channel, "which is", streamURL)
		bufferSize := int64(32767)
		outFile, outErr := os.OpenFile(ne.getNewPath(path), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
		if outErr != nil {
			log.Fatal(outErr)
			return outErr
		}
		streamOut := bufio.NewWriter(outFile)
		streamRes, streamErr := http.Get(streamURL)
		if streamErr != nil {
			log.Fatal(streamErr)
			return streamErr
		}
		buf := bufio.NewReader(streamRes.Body)
		ne.env.tunerPathsToSize[path] = 0
		defer func() {
			// do teardown
			streamRes.Body.Close()
			streamOut.Flush()
			outFile.Close()
			delete(ne.env.tunerPathsToSize, path)
		}()
		log.Println("Executing > fetching ...")
		for {
			if shouldStop() {
				break
			}
			n, err := io.CopyN(streamOut, buf, bufferSize)
			if err != nil {
				log.Fatal(err)
				return err
			}
			if n > 0 {
				streamOut.Flush()
				ne.env.tunerPathsToSize[path] += int(n)
			}
		}
		log.Println("Destroying > Fetching channel", channel, "which is", streamURL)
		return nil // any errors?
	})
}

func (ne *NetworkEncoder) buffer(command string) []string {
	// BUFFER go hdhr prime 1 TV Tuner|458581757|2|16777216|/var/media/tv/asdf-0.mpgbuf|Great
	params := getParams("BUFFER (?P<TunerName>[^|]+)\\|(?P<Nonce>\\d+)\\|(?P<Channel>\\d+)\\|(?P<Sigil>\\d+)\\|(?P<Path>[^|]+)\\|(?P<Quality>[^|]+).*", strings.Trim(command, " "))
	log.Println(params)
	channel, err := strconv.Atoi(params["Channel"])
	if err != nil {
		return ne.fail()
	}
	ne.env.tunerNamesToTasks[params["TunerName"]] = ne.openChannel(channel, params["Path"])
	return ne.ok()
}

func (ne *NetworkEncoder) getFileSize(command string) []string {
	// 'GET_FILE_SIZE /var/media/tv/asdf-0.mpgbuf'
	path := strings.TrimSpace(strings.SplitN(strings.TrimSpace(command), " ", 2)[1])
	size, found := ne.env.tunerPathsToSize[path]
	if found {
		log.Println("Size:", size)
		return []string{strconv.Itoa(size)}
	}
	log.Println("Size: NA")
	return []string{"0"}
}

func (ne *NetworkEncoder) stop(command string) []string {
	// 'STOP go hdhr prime 1 TV Tuner'
	tunerName := strings.TrimSpace(strings.SplitN(strings.TrimSpace(command), " ", 2)[1])
	task, found := ne.env.tunerNamesToTasks[tunerName]
	if found {
		task.Stop()
		delete(ne.env.tunerPathsToSize, tunerName)
	}
	return ne.ok()
}

func (ne *NetworkEncoder) execute(line string) []string {
	data := make([]string, 0)
	line = strings.TrimSpace(line)
	log.Printf("Executing: '%s'\n", line)
	switch strings.Split(line, " ")[0] {
	case "PROPERTIES":
		data = append(data, ne.properties()...)
	case "VERSION":
		data = append(data, string(strconv.Itoa((*ne.env.globalEnvironment).versionMajor)+"."+strconv.Itoa((*ne.env.globalEnvironment).versionMinor)+"."+strconv.Itoa((*ne.env.globalEnvironment).versionDev)))
	case "NOOP":
		data = append(data, ne.ok()...)
	case "START":
		data = append(data, ne.start(line)...)
	case "BUFFER":
		data = append(data, ne.buffer(line)...)
	case "STOP":
		data = append(data, ne.stop(line)...)
	case "GET_FILE_SIZE":
		data = append(data, ne.getFileSize(line)...)
	case "q":
		panic("Quitting")
	default:
		log.Printf("Unhandled: '%s'\n", line)
	}
	return data
}

func (ne *NetworkEncoder) handle(conn net.Conn) {
	log.Printf("Handling connection: %s from %+v\n", conn, *ne)
	defer conn.Close()
	for {
		line, err := readLine(conn)
		if err == nil {
			if strings.TrimSpace(line) == "QUIT" {
				break
			}
			for _, response := range ne.execute(line) {
				log.Println("Writing:", response)
				sendAll(conn, string(response+"\r\n"))
			}
		} else {
			log.Println("Error received:", err)
			break
		}
	}
}

func (ne *NetworkEncoder) listen() {
	log.Printf("Created encoder: %+v\n", *ne)
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(ne.port))
	defer ln.Close()
	check(err)
	log.Println("Encoder service", ne.name, "running...")
	for {
		conn, err := ln.Accept()
		if err == nil {
			go ne.handle(conn)
		}
	}
	log.Println("Broadcast service has stopped running.")
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func broadcastService(port int, encoders []NetworkEncoder, wg *sync.WaitGroup) {
	log.Println("Starting broadcast service...")
	ln, err := net.ListenPacket("udp", ":"+strconv.Itoa(port))
	check(err)
	defer ln.Close()
	log.Println("Broadcast service running...")
	for {
		buffer := make([]byte, 128)
		n, addr, err := ln.ReadFrom(buffer)
		if err == nil && n > 0 {
			log.Println("Got UDP packet: ", string(buffer))
			go func() {
				for _, encoder := range encoders {
					reply := make([]byte, 0)
					reply = append(reply, byte(83), byte(84), byte(78))                                                                                                                                // the first 3 bytes are the "magic" signature
					reply = append(reply, byte((*encoder.env.globalEnvironment).versionMajor), byte((*encoder.env.globalEnvironment).versionMinor), byte((*encoder.env.globalEnvironment).versionDev)) // now append the major.minor.dev; e.g. 4.1.0
					reply = append(reply, byte((encoder.port>>8)&255), byte(encoder.port&255))                                                                                                         // now send the encoder port
					reply = append(reply, byte(len(encoder.name)&255))                                                                                                                                 // now the encoder name's payload length
					reply = append(reply, []byte(encoder.name)...)                                                                                                                                     // now the encoder name
					log.Println("Sending bytes: ", reply, "to", addr)
					ln.WriteTo(reply, addr)
				}
			}()
		}
	}
	log.Println("Broadcast service has stopped running.")
}

func main() {
	globalEnv := GlobalEnvironmentVariables{}

	flag.StringVarP(&globalEnv.baseName, "base-name", "bn", "go hdhr prime", "base name for the encoders")
	flag.StringVarP(&globalEnv.encoderID, "encoder-id", "id", "1234569", "encoder ID as a number")
	flag.StringVarP(&globalEnv.hdhrIP, "hdhr-ip", "ip", "localhost", "this is the ip of the HDHR encoder; do not leave blank.")
	flag.IntVarP(&globalEnv.numEncoders, "num-encoders", "n", 3, "number of hdhr encoders")
	flag.StringVarP(&globalEnv.pathSearch, "path-search", "ps", "/var/media/tv/", "this is the needle in the haystack")
	flag.StringVarP(&globalEnv.pathReplace, "path-replace", "pr", "/var/media/tv/", "(...and this is the haystack) if found in path search, the path to replace")
	flag.IntVarP(&globalEnv.startPort, "start-port", "p", 31336, "the first encoder will have this value + 1, then + 2, then + 3, then + ... num-encoders")
	flag.IntVarP(&globalEnv.versionMajor, "version-major", "va", 4, "the major version to report to sage tv, i.e. the 'a' in a.b.c")
	flag.IntVarP(&globalEnv.versionMinor, "version-minor", "vb", 1, "the minor version to report to sage tv, i.e. the 'b' in a.b.c")
	flag.IntVarP(&globalEnv.versionDev, "version-dev", "vc", 0, "the dev version to report to sage tv, i.e. the 'c' in a.b.c")

	flag.Parse()

	env := EncoderEnvironments{
		globalEnvironment: &globalEnv,
		tunerNamesToTasks: make(map[string]*runner.Task),
		tunerPathsToSize:  make(map[string]int)}
	log.Printf("Configuration/Environment: %+v\n", env)

	log.Println("Creating encoders...")
	encoders := make([]NetworkEncoder, 0)
	for i := 1; i <= globalEnv.numEncoders; i++ {
		encoder := NetworkEncoder{name: globalEnv.baseName + " " + strconv.Itoa(i), port: globalEnv.startPort + i, env: &env, lineup: nil}
		encoders = append(encoders, encoder)
		go encoder.listen()
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go broadcastService(8271, encoders, &wg)

	wg.Wait()
	log.Println("Done with server...")
}