import io from "socket.io-client";

let socket = io.connect("http://localhost:8080");

export default socket;