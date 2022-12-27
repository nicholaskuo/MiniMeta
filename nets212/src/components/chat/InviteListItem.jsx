import axios from 'axios';
import './InviteListItem.css';

function InviteListItem({invite, invites, setInvites, chatrooms, setChatrooms}) {

    const joinChat = async () => {
        if (chatrooms.filter(x => x.chatID === invite.chatID).length === 0) {

            var params = {
                username : localStorage.getItem("user"),
                chatID: invite.chatID,
                members : invite.inviters,
            }
            
            // Add current user to chat
            axios.post('http://localhost:8080/addToChat', params).then(async response => {
                console.log(response.data);
                if (response.data.message === "Success") {

                    // Retrieve messages for the joined chatroom
                    let response = await fetch(`http://localhost:8080/getMessages?room=${encodeURIComponent(invite.chatID)}`);
                    let data = await response.json();
                    var cr = {
                        chatID: invite.chatID,
                        members: invite.inviters,
                        messages: data
                    }
                    chatrooms.push(cr);
                    setChatrooms(chatrooms.map(x => x));
                    deleteInvite();
                }
            })
        } else {
            deleteInvite();
        }
    }

    // Delete the accepted chat invite in the backend
    const deleteInvite = () => {
        setInvites(invites.filter(x => x !== invite))
        var params = {
            username: localStorage.getItem("user"),
            chatID: invite.chatID,
        }
        axios.post('http://localhost:8080/deleteChatInvite', params);
    }

    return (
        <div className = "chat-invite-container">
            <div className = "chat-invite-info-container">
                {invite.inviters.join(", ")}
            </div>
            <div className = 'chat-invite-buttons-container'>
                <button type = "button" onClick = {() => joinChat()}
                    className = "chat-accept-button btn btn-success">
                    Accept
                </button>
                <button type = "button" onClick = {() => deleteInvite()}
                    className = "chat-decline-button btn btn-danger">
                    Decline
                </button>
            </div>
            
        </div>
    )
}

export default InviteListItem