package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)


var client_map = make(map[string]*net.TCPConn)
var Port string
var nblist []string                       //introduce ip list
var cnmap = make(map[string]*net.TCPConn) //save connect object
var acmap = make(map[string]string)       //try to connect to all node, this is the map of not connect
var transmap = make(map[string]string)    // receive transaction map

func Encode(message string) ([]byte, error) {

	var length int32 = int32(len(message))
	var pkg *bytes.Buffer = new(bytes.Buffer)

	err := binary.Write(pkg, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}

	err = binary.Write(pkg, binary.LittleEndian, []byte(message))
	if err != nil {
		return nil, err
	}

	return pkg.Bytes(), nil
}

func Decode(reader *bufio.Reader) (string, error) {

	lengthByte, _ := reader.Peek(4)
	lengthBuff := bytes.NewBuffer(lengthByte)
	var length int32
	err := binary.Read(lengthBuff, binary.LittleEndian, &length)
	if err != nil {
		return "", err
	}
	if int32(reader.Buffered()) < length+4 {
		return "", err
	}


	pack := make([]byte, int(4+length))
	_, err = reader.Read(pack)
	if err != nil {
		return "", err
	}
	return string(pack[4:]), nil
}


func listen_client() {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:0")
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Print("CAN'T LISTEN: ", err)
		return
	}
	port := tcpListener.Addr().(*net.TCPAddr).Port
	Port = strconv.Itoa(port)
	myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")
	myipaddr := ip[0] + ":" + Port
	acmap[myipaddr] = "node2"
	for { 
		client_con, _ := tcpListener.AcceptTCP()                
		client_map[client_con.RemoteAddr().String()] = client_con 
		go add_receiver(client_con)
		fmt.Println(client_con.RemoteAddr().String() + "connect")
	}
}


func add_receiver(current_connect *net.TCPConn) {
	for {
		/*byte_msg := make([]byte, 2048)
		len, err := current_connect.Read(byte_msg)
		if err != nil {
			current_connect.Close()
			fmt.Println("server left", current_connect.RemoteAddr().String())
			break
		}
		receive_msg := string(byte_msg[:len])*/
		receive_msg, err := Decode(bufio.NewReader(current_connect))

		if err != nil {
			current_connect.Close()
			delete(cnmap, current_connect.RemoteAddr().String())
			for i := range nblist {
				if nblist[i] == current_connect.RemoteAddr().String() {
					nblist = append(nblist[:i], nblist[i+1:]...)
					break
				}
			}
			fmt.Println("server left", current_connect.RemoteAddr().String())
			break
		}
		receive_msg_split := strings.Split(receive_msg, "\n")
		for j := range receive_msg_split {
			flagn := true
			flagt := true
			head := strings.Split(receive_msg_split[j], " ")
			if head[0] == "node" {
				for i := range nblist {
					if head[1] == nblist[i] {
						flagn = false
						break
					}
				}
				for i := range acmap {
					if head[1] == i {
						flagn = false
						break
					}
				}
				if flagn {
					acmap[head[1]] = "node"
					fmt.Println("Introduce" + receive_msg_split[j])

				}
			}

			if head[0] == "TRANSACTION" {
				for i := range transmap {
					if receive_msg_split[j] == i {
						flagt = false
						break
					}
				}
				if flagt {
					t := time.Now().Nanosecond() //add a timestamp for message
					ct := strconv.Itoa(t)
					transmap[receive_msg_split[j]] = ct
					//fmt.Println("log��" + receive_msg_split[j] + " timestamp: " + ct)
					fmt.Println(receive_msg_split[j] + " timestamp: " + ct)

					trans_gossip()
				}
			}
		}
	}
}

//gossip node information
func node_gossip() {
	myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")
	myipaddr := ip[0] + ":" + Port
	//fmt.Print("node_gossip ip" + myipaddr)
	var str string
	var rannum []int
	var node_msg []byte

	if len(cnmap) < 10 {
		for i := range cnmap {
			for k := range nblist {
				str = "node " + nblist[k] + "\n"
				node_msg, _ = Encode(str)
				cnmap[i].Write(node_msg)
			}
			for k := range acmap {
				str = "node " + k + "\n"
				node_msg, _ = Encode(str)
				cnmap[i].Write(node_msg)
			}
		}
	} else {
		rand.Seed(time.Now().Unix())
		for i := 0; i < 9; {
			flag := true
			rnd := rand.Intn(len(cnmap))
			for j := range rannum {
				if rnd == rannum[j] || nblist[rnd] == myipaddr { 
					flag = false
					break
				}
			}
			if flag {
				i++
				rannum = append(rannum, rnd)
				//fmt.Print("rnd:", rnd)
				for j := range cnmap {
					if nblist[rnd] == j {
						for k := range nblist {
							str = "node " + nblist[k] + "\n"
							node_msg, _ = Encode(str)
							//fmt.Print("send to" + j + str)
							cnmap[j].Write(node_msg)
						}
						for k := range acmap {
							str = "node " + k + "\n"
							node_msg, _ = Encode(str)
							//fmt.Print("send to" + j + str)
							cnmap[j].Write(node_msg)
						}

					}
				}
			}

		}

	}
	time.Sleep(1 * time.Second)
}

//gossip transaction information
func trans_gossip() {
	time.Sleep(1 * time.Second)
	myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")
	myipaddr := ip[0] + ":" + Port
	var str string
	var rannum []int
	var trans_msg []byte

	if len(cnmap) < 4 {
		for i := range cnmap {
			for k := range transmap {
				str = k + "\n"
				trans_msg, _ = Encode(str)
				cnmap[i].Write(trans_msg)
			}
		}
	} else {
		rand.Seed(time.Now().Unix())
		for i := 0; i < 3; {
			flag := true
			rnd := rand.Intn(len(cnmap))
			for j := range rannum {
				if rnd == rannum[j] || nblist[rnd] == myipaddr {
					flag = false
					break
				}
			}
			if flag {
				i++
				rannum = append(rannum, rnd)
				for j := range cnmap {
					if nblist[rnd] == j {
						for k := range transmap {
							str = k + "\n"
							trans_msg, _ = Encode(str)
							cnmap[j].Write(trans_msg)
						}

					}
				}
			}

		}

	}
	time.Sleep(1 * time.Second)
}
func one_msg_gossip(str string) {
	myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")
	myipaddr := ip[0] + ":" + Port
	var rannum []int
	var trans_msg []byte

	if len(cnmap) < 4 {
		for i := range cnmap {
			trans_msg, _ = Encode(str)
			cnmap[i].Write(trans_msg)
		}
	} else {
		rand.Seed(time.Now().Unix())
		for i := 0; i < 3; {
			flag := true
			rnd := rand.Intn(len(cnmap))
			for j := range rannum {
				if rnd == rannum[j] || nblist[rnd] == myipaddr {
					flag = false
					break
				}
			}
			if flag {
				i++
				rannum = append(rannum, rnd)
				for j := range cnmap {
					if nblist[rnd] == j {
						trans_msg, _ = Encode(str)
						cnmap[j].Write(trans_msg)
					}
				}
			}

		}

	}
}


func connect_service(addr string) {
	tcp_addr, _ := net.ResolveTCPAddr("tcp", addr) 
	con, err := net.DialTCP("tcp", nil, tcp_addr)  
	if err != nil {
		fmt.Println("CAN NOT CONNECT TO SERVICE")
		os.Exit(1)
	}
	go connect_sender(con)
	go connect_receiver(con)

}


func connect_receiver(self_connect *net.TCPConn) {
	buff := make([]byte, 2048)
	for {
		len, err := self_connect.Read(buff) 
		if err != nil {
			fmt.Print("service unconnect")
			break
		}
		msg := string(buff[:len])
		sersend := strings.Split(msg, "\n")
		for i := range sersend {
			head := strings.Split(sersend[i], " ")
			if head[0] == "INTRODUCE" {
				acmap[head[2]+":"+head[3]] = head[1]
				fmt.Println(sersend[i]) 
			}
			if head[0] == "TRANSACTION" {
				/*trans := strings.Split(sersend[i], " ")
				var transaction Transaction
				transaction.timestamp = trans[1]
				transaction.transid = trans[2]
				transaction.sournum = trans[3]
				transaction.desnnum = trans[4]
				transaction.amount = trans[5]
				transmap[trans[2]] = transaction*/
				t := time.Now().Nanosecond() //add a timestamp for message
				ct := strconv.Itoa(t)
				transmap[sersend[i]] = ct
				//fmt.Println("log��" + sersend[i] + " timestamp: " + ct)
				fmt.Println(sersend[i] + " timestamp: " + ct)
				one_msg_gossip(sersend[i])
			}
			if head[0] == "QUIT" || head[0] == "DIE" {
				fmt.Println("QUIT or DIE")
				os.Exit(1)
			}
		}
		//fmt.Println(msg)
	}
}

// connect service
func connect_sender(self_connect *net.TCPConn) {
	myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")
	coninfo := "CONNECT\x20node1\x20" + ip[0] + "\x20" + Port + "\n"
	read_line_msg := []byte(coninfo)
	self_connect.Write(read_line_msg)

}

func connect() {
	for {
		if len(acmap) > 0 { 
			for key := range acmap {
				tcp_addr, _ := net.ResolveTCPAddr("tcp", key) 
				con, err := net.DialTCP("tcp", nil, tcp_addr) 
				if err != nil {
					fmt.Println("%s cannot connect", key)
					delete(acmap, key)
					continue
				}
				//fmt.Println("connect success", key)
				delete(acmap, key)
				cnmap[key] = con
				nblist = append(nblist, key)
				first_connect_send(con)
				node_gossip()
				fmt.Println("nblist", nblist)
			}

		}
		time.Sleep(1 * time.Second)
	}

}
func first_connect_send(self_connect *net.TCPConn) {
	var node_msg string
	var tran_msg string
	for i := range nblist { //send full node information
		node_msg = "node " + nblist[i] + "\n"
		new_msg, _ := Encode(node_msg)
		self_connect.Write(new_msg)
	}
	for i := range acmap {
		node_msg = "node " + i + "\n"
		new_msg, _ := Encode(node_msg)
		self_connect.Write(new_msg)
	}
	for i := range transmap {
		tran_msg = i + "\n"
		new_msg, _ := Encode(tran_msg)
		self_connect.Write(new_msg)
	}

	time.Sleep(1 * time.Second)
}


func main() {
	/*myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")package main
	myipaddr := ip[0] + ":8080"
	acmap[myipaddr] = "node"*/
	go listen_client()
	time.Sleep(1 * time.Second)
	go connect()
	time.Sleep(1 * time.Second)
	connect_service("172.22.94.28:8080")
	time.Sleep(1 * time.Second)

	select {}
}
