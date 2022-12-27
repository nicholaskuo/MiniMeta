import React, {useState, useEffect} from 'react';
import { Navigate } from 'react-router-dom';
import axios from 'axios';
import io from "socket.io-client";
import './Chat.css';
import FriendListItem from './FriendListItem'
import InviteListItem from './InviteListItem'
import ChatListItem from './ChatListItem'
import ChatBox from './ChatBox'
import socket from "./socket"
import "bootstrap/dist/css/bootstrap.min.css";
import Header from "../header/Header";

function Chat() {
    const auth = localStorage.getItem("authenticated");
    const user = localStorage.getItem("user");


    let [chatrooms, setChatrooms] = useState([]);

    let [currChatID, setCurrChatID] = useState("");

    let [currChatKey, setCurrChatKey] = useState(-1);

    let [friends, setFriends] = useState([]);

    let [invites, setInvites] = useState([]);

    let [showChat, setShowChat] = useState(false);

    // On loading, get a user's friends, chatrooms and invites
    useEffect(() => {

        getFriends();

        // Funcition to get all chatrooms of a user and their message histories
        async function fetchChatrooms() {
            let response = await fetch(`http://localhost:8080/getChats?user=${encodeURIComponent(localStorage.getItem("user"))}`);
            let chats = await response.json();
            var crArr = []

            for (var i = 0; i < chats.length; i++) {
                let response = await fetch(`http://localhost:8080/getMessages?room=${encodeURIComponent(chats[i].chatID)}`);
                let data = await response.json();
                var cr = {
                    chatID: chats[i].chatID,
                    members: chats[i].members,
                    messages: data
                }
                crArr.push(cr);
            }

            setChatrooms(crArr);
        }

        fetchChatrooms();
        getInvites();
        
        let interval = setInterval(() => {
            getInvites();
        }, 3000);

        return () => {
            clearInterval(interval);
        };
    }, [])

    // Funcition to get a user's invites
    let getInvites = async () => {
        if (localStorage.getItem("user") !== null) {
            var params = {
                username: localStorage.getItem("user")
            }
            axios.post('http://localhost:8080/getChatInvites', params).then(val => {
                setInvites(val.data);
            })
        }
    }

    // Function to retrieve a user's list of friends and their online status from the backend
    let getFriends = async () => {
        if (localStorage.getItem("user") !== null) {
            var params = {username : localStorage.getItem("user")}
            axios.post('http://localhost:8080/getUserFriendForChat', params).then(val => {
                if (val !== null) {
                    function compare( a, b ) {
                        if ( a.online > b.online ){
                          return -1;
                        }
                        if ( a.online < b.online ){
                          return 1;
                        }
                        return 0;
                    }
                    setFriends(val.data.sort(compare));
                }
            })
        }
    }

    // Function to open a chatroom from the list of chatrooms
    let openChatroom = async (prevChat, chatID, key) => {
        setCurrChatID(chatID + "");
        setCurrChatKey(key); 
        setShowChat(true);

        if (prevChat !== "") {
            socket.emit("leave_room", prevChat);
        }
        socket.emit("join_room", chatID);
    }

    // Return div
    if (!auth) {
        return <Navigate to = "/login"/>;
    } else {
        return (
            <div>
                <Header />
            <div className = "chat-page-container">
            <div className = "chat-total-container">
                <div className = "left-container">
                    <div className = "friends-container">
                        <div className = "friends-title">Friends</div>
                        {friends.map((friend, index) => (
                            <FriendListItem
                                key = {index}
                                last = {index === friends.length - 1}
                                friend = {friend}
                                chatrooms = {chatrooms}
                                setChatrooms = {setChatrooms}
                            />
                        ))}
                    </div>
                    <div className = "invites-container">
                        <div className = "invites-title">Invites</div>
                        {invites.map((invite, i) => (
                            <InviteListItem
                                key = {i}
                                invite = {invite}
                                invites = {invites}
                                setInvites = {setInvites}
                                chatrooms = {chatrooms}
                                setChatrooms = {setChatrooms}
                            />
                        ))}
                    </div>
                </div>
                <div className = "chatrooms-chatbox-container">
                    <div className = "chatrooms-container">
                        <div className = "chatrooms-title">Chats for {user}</div>
                            {chatrooms.map((room, i) => (
                                <ChatListItem
                                    key = {i}
                                    index = {i}
                                    chatroom = {room}
                                    openChatroom = {openChatroom}
                                    currChatID = {currChatID}
                                />
                            ))}

                    </div>
                    <div className = "chatbox-container">
                        <ChatBox 
                            currChatKey = {currChatKey} 
                            chatrooms = {chatrooms}
                            friends = {friends}
                            currChatID = {currChatID}
                            setChatrooms = {setChatrooms}
                            socket = {socket}
                            showChat = {showChat}
                            setShowChat = {setShowChat}
                            setCurrChatKey = {setCurrChatKey}
                        />
                    </div>
                </div>
                
            </div>
        </div>
        </div>
        )
    }
}

export default Chat; 