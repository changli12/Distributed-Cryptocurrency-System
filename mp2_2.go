package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var service_connect *net.TCPConn
var Port string
var nblist []string                       //introduce ip list
var cnmap = make(map[string]*net.TCPConn) //save connect object
var acmap = make(map[string]string)       //try to connect to all node, this is the map of not connect
var transmap = make(map[string]string)    // receive transaction map
var accountmap = make(map[string]int)     // transaction account
var usetransmap = make(map[string]string)
var blocklist []Block1
var verifymap = make(map[string]string)

type Block1 struct {
	index    int
	head     string
	body     string
	solution string
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
		go add_receiver(client_con)
		fmt.Println(client_con.RemoteAddr().String() + "connect")
	}
}

func add_receiver(current_connect *net.TCPConn) {
	for {

		byte_msg := make([]byte, 8000)
		len1, err := current_connect.Read(byte_msg)
		if err != nil {
			current_connect.Close()
			fmt.Println("server left", current_connect.RemoteAddr().String())
			break
		}
		receive_msg := string(byte_msg[:len1])

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
			time.Sleep(1 * time.Second)
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
					usetransmap[receive_msg_split[j]] = ct
					fmt.Println(receive_msg_split[j] + " timestamp: " + ct)
					trans_gossip()
				}
			}
			if head[0] == "BLOCK" {
				Blockbody := receive_msg_split[j][6:]
				fmt.Println(Blockbody)
				blockinfo := strings.Split(Blockbody, "#")
				if len(blockinfo) > len(blocklist) {
					blocklist = append([]Block1{})
					usetransmap = transmap
					for b := range blockinfo {
						blockdetail := strings.Split(blockinfo[b], "@")
						var block Block1
						block.index, _ = strconv.Atoi(blockdetail[0])
						block.head = blockdetail[1]
						block.body = blockdetail[2]
						blocktrans := strings.Split(blockdetail[2], "!")
						block.solution = blockdetail[3]
						blocklist = append(blocklist, block)
						for t := range blocktrans {
							delete(usetransmap, blocktrans[t])
						}

					}

				}
				fmt.Println(receive_msg_split[j])
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
	var str string
	var rannum []int
	node_msg := make([]byte, 200)
	if len(cnmap) < 10 {
		for i := range cnmap {
			for k := range nblist {
				str = "node " + nblist[k] + "\n"
				copy(node_msg, []byte(str))
				cnmap[i].Write(node_msg)
			}
			for k := range acmap {
				str = "node " + k + "\n"
				copy(node_msg, []byte(str))
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
				for j := range cnmap {
					if nblist[rnd] == j {
						for k := range nblist {
							str = "node " + nblist[k] + "\n"
							copy(node_msg, []byte(str))
							cnmap[j].Write(node_msg)
						}
						for k := range acmap {
							str = "node " + k + "\n"
							copy(node_msg, []byte(str))
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
	trans_msg := make([]byte, 200)

	if len(cnmap) < 4 {
		for i := range cnmap {
			for k := range transmap {
				str = k + "\n"
				copy(trans_msg, []byte(str))
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
							copy(trans_msg, []byte(str))
							cnmap[j].Write(trans_msg)
						}

					}
				}
			}

		}

	}
	time.Sleep(1 * time.Second)
}

func block_gossip() {
	time.Sleep(1 * time.Second)
	myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")
	myipaddr := ip[0] + ":" + Port
	var str string
	var rannum []int
	trans_msg := make([]byte, 4000)
	str = "BLOCK "
	for k := 0; k < len(blocklist); k++ {
		str += strconv.Itoa(k) + "@" + blocklist[k].head + "@" + blocklist[k].body + "@" + blocklist[k].solution + "@#"
	}
	str += "\n"
	copy(trans_msg, []byte(str))
	if len(cnmap) < 4 {
		for i := range cnmap {
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
						cnmap[j].Write(trans_msg)

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
	trans_msg := make([]byte, 200)
	copy(trans_msg, []byte(str))
	if len(cnmap) < 4 {
		for i := range cnmap {
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
						cnmap[j].Write(trans_msg)
					}
				}
			}

		}

	}
}

// 建立连接
func connect_service(addr string) {
	tcp_addr, _ := net.ResolveTCPAddr("tcp", addr)
	con, err := net.DialTCP("tcp", nil, tcp_addr)
	if err != nil {
		fmt.Println("CAN NOT CONNECT TO SERVICE")
		os.Exit(1)
	}
	service_connect = con
	go connect_sender(con)
	go connect_receiver(con)

}

// 消息接收器
func connect_receiver(self_connect *net.TCPConn) {
	buff := make([]byte, 2048)
	for {
		len1, err := self_connect.Read(buff)
		if err != nil {
			fmt.Print("service unconnect")
			break
		}
		msg := string(buff[:len1])
		sersend := strings.Split(msg, "\n")
		for i := range sersend {
			head := strings.Split(sersend[i], " ")
			if head[0] == "INTRODUCE" {
				acmap[head[2]+":"+head[3]] = head[1]
				fmt.Println(sersend[i])
			}
			if head[0] == "TRANSACTION" {
				t := time.Now().Nanosecond() //add a timestamp for message
				ct := strconv.Itoa(t)
				transmap[sersend[i]] = ct
				usetransmap[sersend[i]] = ct
				fmt.Println(sersend[i] + " timestamp: " + ct)
				one_msg_gossip(sersend[i] + "\n")
			}
			if head[0] == "QUIT" || head[0] == "DIE" {
				fmt.Println("QUIT or DIE")
				os.Exit(1)
			}
			if head[0] == "SOLVED" {
				var tempblock Block1
				tempblock.index = len(blocklist)
				tempblock.head = head[1]
				tempblock.solution = head[2]
				addtransaction(tempblock)

			}
			if head[0] == "VERIFY" {
				verifymap[head[2]+head[3]] = head[1]

			}
		}
	}
}
func addtransaction(block Block1) {
	i := 0
	for trans := range usetransmap {
		i++
		tx := strings.Split(trans, " ")
		money, _ := strconv.Atoi(tx[5])
		if tx[3] == "0" {
			accountmap[tx[4]] += money
			block.body += trans + "!"
		} else {
			if accountmap[tx[3]]-money > 0 {
				accountmap[tx[3]] -= money
				block.body += trans + "!"
				delete(usetransmap, trans)
			}
		}
		if i > 15 {
			break
		}
	}
	go addblock(block)
	time.Sleep(1 * time.Second)
}

func addblock(block Block1) {
	if block.head == "86777674e3fe09e0da911be4c7bce219794a8988955508d3e9433d8584630b1f" {
		verify_sender(block.head, block.solution)
		for {
			time.Sleep(1 * time.Second)
			if verifymap[block.head+block.solution] == "OK" { //verify receiver
				blocklist = append(blocklist, block)
				solve_sender(block.solution)
				block_gossip()
				fmt.Println("verify succeed" + block.head)
				break
			}
			if verifymap[block.head+block.solution] == "FAIL" { //verify receiver
				fmt.Println("verify fail" + block.head)
				break
			}
		}
		time.Sleep(1 * time.Second)

	} else {
		if blocklist[len(blocklist)-1].solution == block.head {
			verify_sender(block.head, block.solution)
			for {
				if verifymap[block.head+block.solution] == "OK" { //verify receiver
					blocklist = append(blocklist, block)
					solve_sender(block.solution)
					block_gossip()
					fmt.Println("verify succeed" + block.head)
					break
				}
				if verifymap[block.head+block.solution] == "FAIL" { //verify receiver
					fmt.Println("verify fail" + block.head)
					time.Sleep(1 * time.Second)
					break
				}
			}

		}
	}

}

func connect_sender(self_connect *net.TCPConn) {
	myip, _ := net.InterfaceAddrs()
	myIp := myip[1].String()
	ip := strings.Split(myIp, "/")
	coninfo := "CONNECT\x20node1\x20" + ip[0] + "\x20" + Port + "\n"
	read_line_msg := []byte(coninfo)
	self_connect.Write(read_line_msg)
	solve_sender("86777674e3fe09e0da911be4c7bce219794a8988955508d3e9433d8584630b1f")

}

func solve_sender(puzzle string) {
	coninfo := "SOLVE\x20" + puzzle + "\n"
	read_line_msg := []byte(coninfo)
	service_connect.Write(read_line_msg)
}
func verify_sender(puzzle string, solution string) {
	coninfo := "VERIFY\x20" + puzzle + "\x20" + solution + "\n"
	read_line_msg := []byte(coninfo)
	service_connect.Write(read_line_msg)
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
	node_msg := make([]byte, 200)
	trans_msg := make([]byte, 200)
	var str string
	for i := range nblist { //send full node information
		str = "node " + nblist[i] + "\n"
		copy(node_msg, []byte(str))
		self_connect.Write(node_msg)
	}
	for i := range acmap {
		str = "node " + i + "\n"
		copy(node_msg, []byte(str))
		self_connect.Write(node_msg)
	}
	for i := range transmap {
		str = i + "\n"
		copy(trans_msg, []byte(str))
		self_connect.Write(trans_msg)
	}

	time.Sleep(1 * time.Second)
}

func main() {
	go listen_client()
	time.Sleep(1 * time.Second)
	go connect()
	time.Sleep(1 * time.Second)
	connect_service("172.22.94.28:8080")
	time.Sleep(1 * time.Second)

	select {}
}
