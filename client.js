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
    return await new Promise((resolve) => {
      socket.onopen = () => resolve(client);
    });
  }

  #message_recv(e) {
    const data = JSON.parse(e.data);
    if (data.SubscriptionUpdate) {
      const [key, value] = data.SubscriptionUpdate;
      for (const subscriber of this.subscribers[key]) {
        subscriber(value);
      }
    } else if ("Error" in data) {
      this.next_value?.({ error: data.Error });
    } else {
      this.next_value?.({ value: data.Value });
    }
  }

  async #wait_next_value() {
    const message = await new Promise((resolve) => (this.next_value = resolve));
    if (message.error) {
      throw new Error(message.error);
    } else {
      return message;
    }
  }

  async get(key) {
    this.socket.send(JSON.stringify({ Get: key }));
    return await this.#wait_next_value();
  }

  async set(key, value) {
    this.socket.send(JSON.stringify({ Set: [key, value] }));
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
