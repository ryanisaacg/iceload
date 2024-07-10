class IceloadClient {
  constructor(url) {
    this.socket = new WebSocket(url);
    //this.send_buffer = [];
    this.is_ready = new Promise(resolve => this.socket.onopen = resolve);
  }

  async #next_value() {
    // TODO: this ain't right
    const event = await new Promise(resolve => this.socket.onmessage = resolve);
    return event.data;
  }

  async get(key) {
    await this.is_ready;
    this.socket.send(JSON.stringify({ Get: key}));
    return await this.#next_value();
  }

  async set(key, value) {
    await this.is_ready;
    this.socket.send(JSON.stringify({ Set: [key, value]}));
    return await this.#next_value();
  }
}
