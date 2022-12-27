
import axios from 'axios';
import { useRef, useEffect, useState } from 'react'
import './ChatBox.css'
import sendLogo from './send-button.png';
import ChatMessageItem from './ChatMessageItem';


function ChatBox({setCurrChatKey, showChat, setShowChat, chatrooms, currChatID, 
    currChatKey, friends, setChatrooms, socket}) {

    let [chatHeader, setChatHeader] = useState("");

    let [messages, setMessages] = useState([]);

    let [currentMessage, setCurrentMessage] = useState("");

    let [friendInput, setFriendInput] = useState("");

    let [currChat, setCurrChat] = useState({});

    const messagesEndRef = useRef(null)

    // Listener for handling incoming messages through socket.io
    useEffect(() => {
        socket.on("receive_message", (data) => {
            console.log("Message received");
            setMessages((list) => [...list, data]);
        });
        return () => socket.off('receive_message');
    }, [socket])

    // Listener for updating the chatroom infos on change (ex. if a user joins a chatroom)
    useEffect(() => {
        if (currChatKey >= 0 && chatrooms && currChatKey < chatrooms.length) {
            setChatHeader(chatrooms[currChatKey].members.join(", "));
            setCurrChat(chatrooms[currChatKey]);

            let updateMessages = async () => {
                let response = await fetch(`http://localhost:8080/getMessages?room=${
                    encodeURIComponent(chatrooms[currChatKey].chatID)}`);
                let messages = await response.json();
                setMessages(messages);
            }

            updateMessages();
        }
    }, [chatrooms, currChatKey])

    // Listener for updating local storage of messages on incoming message
    useEffect(() => {

        if (currChatKey >= 0) {
            chatrooms[currChatKey].messages = messages;
        }
        setChatrooms(chatrooms);

        messagesEndRef.current?.scrollIntoView();
    }, [messages])
    
    // Function to send message through socket io
    let sendMessage = () => {
        if (currentMessage !== "" && currChatKey !== -1) {
          const messageData = {
            room: currChatID,
            author: localStorage.getItem("user"),
            content: currentMessage,
          };
            socket.emit("send_message", messageData);
            setCurrentMessage("");
        }
    };

    // Function to add a friend to an existing chatroom (thereby creating a new chatroom)
    let addFriendToChat = async () => {


        // Check if the invited user is a friend of the current user
        if (friends && friends.find(x => x.user === friendInput) !== undefined 
            && friends !== localStorage.getItem("user") && currChat.members.find(x => x === friendInput) === undefined) {
            var allChatMembers = structuredClone(currChat.members);
            allChatMembers.push(localStorage.getItem("user"));
            allChatMembers.push(friendInput);
            allChatMembers.sort();
            var crID = allChatMembers.join("");
            
            // Check if the new chatroom doesn't already exist
            if (chatrooms.find(x => x.chatID === crID) === undefined) {

                // Send invites to each chatroom member
                allChatMembers.forEach((memberID) => {
                    if (memberID !== localStorage.getItem("user")) {
                        var members = allChatMembers.filter(x => x !== memberID);
    
                        var params = {
                            username : memberID,
                            chatID: crID,
                            members : members,
                        }
                        axios.post('http://localhost:8080/inviteToChat', params);
                    }
                })
    
                var params = {
                    username : localStorage.getItem("user"),
                    chatID : crID,
                    members : allChatMembers.filter(x => x !== localStorage.getItem("user")),
                }
    
                // Add current user to the chatroom in the backend 
                axios.post('http://localhost:8080/addToChat', params).then(async response => {
                        if (response.data.message === "Success") {
                            let response = await fetch(`http://localhost:8080/getMessages?room=${encodeURIComponent(crID)}`);
                            let data = await response.json();
                            var cr = {
                                chatID: crID,
                                members: allChatMembers.filter(x => x !== localStorage.getItem("user")),
                                messages: data
                            }
                            chatrooms.push(cr);
                            setChatrooms(chatrooms.map(x => x));
                        }
                })
            }
        }
        setFriendInput("");
    }

    // Function to leave a chat
    let leaveChat = () => {
        var params = {
            username: localStorage.getItem("user"),
            chatID: currChatID
        }
        
        // Remove current user from a chatroom in the backend
        axios.post('http://localhost:8080/removeFromChat', params).then(async () => {
            if (currChatKey >= 0 && currChatKey < chatrooms.length) {
                chatrooms.splice(currChatKey, 1);
            }
            setChatrooms(chatrooms.map(x => x));
            setMessages([]);
            setCurrChatKey(-1);
            setChatHeader("");
        })
    }

    return (
        <>
            <div className = "chat-header-container">
                {(currChatKey === -1) ? "" : (
                    <>
                        <div className = "chat-header">
                            {chatHeader}
                        </div>
                        <div className = "addfriend-leave-container">
                            <input
                                type="text"
                                value={friendInput}
                                placeholder="User ID"
                                onChange={(event) => {
                                    setFriendInput(event.target.value);
                                }}
                                onKeyPress={(event) => {
                                    event.key === "Enter" && addFriendToChat();
                                }}
                                className = "friend-input"
                            />
                            <button className = "friend-input-button btn btn-primary" onClick={addFriendToChat}>Invite</button>
                            <button className = "leave-button btn btn-danger" onClick = {leaveChat}>Leave</button>
                        </div>
                    </>
                )}
            </div>
            <div className = "chat-messages">
                {messages.map((message, index) => (
                            <ChatMessageItem key = {index} message = {message}/>
                ))}
                <div ref={messagesEndRef} />
            </div>
            <div className = "chat-input-container">
                <div className = "chat-input-button">
                    <input
                        type="text"
                        value={currentMessage}
                        placeholder="Type your message here"
                        onChange={(event) => {
                            setCurrentMessage(event.target.value);
                        }}
                        onKeyPress={(event) => {
                            event.key === "Enter" && sendMessage();
                        }}
                        className = "chat-input"
                    />
                    <img src={sendLogo} className="chat-send-button" alt = "Send" onClick={sendMessage}/>
                </div>
            </div>
        </>
    )
}

export default ChatBox