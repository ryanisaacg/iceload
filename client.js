class IceloadClient {
  constructor(socket) {
    this.socket = socket;
    this.socket.onmessage = (e) => this.#message_recv(e);
    this.next_value = null;
    this.subscribers = {};
  }

  static async connect(url) {
    const socket = new WebSocket(url);
    const client = new IceloadClient(socket);
    return await new Promise(resolve => {
      socket.onopen = () => resolve(client);
    });
  }

  #message_recv(e) {
    const data = JSON.parse(e.data);
    if (data.ValueChanged) {
      const [key, value] = data.ValueChanged;
      for (const subscriber of this.subscribers[key]) {
        subscriber(value);
      }
    } else if ("Value" in data) {
      this.next_value?.(data.Value);
    }
  }

  async #wait_next_value() {
    return new Promise(resolve => this.next_value = resolve);
  }

  async get(key) {
    this.socket.send(JSON.stringify({ Get: key}));
    return await this.#wait_next_value();
  }

  async set(key, value) {
    this.socket.send(JSON.stringify({ Set: [key, value]}));
    return await this.#wait_next_value();
  }

  async subscribe(key, callback) {
    if (!(key in this.subscribers)) {
      this.socket.send(JSON.stringify({ Subscribe: key }));
      this.subscribers[key] = new Set();
    }
    this.subscribers[key].add(callback);
  }

  async unsubscribe(key, callback) {
    this.subscribers[key].remove(callback);
    if (this.subscribers[key].size === 0) {
      delete this.subscribers[key];
      this.socket.send(JSON.stringify({ Unsubscribe: key }));
    }
  }
}

