package telegram

// Update is one Bot API update.
type Update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *Message       `json:"message"`
	EditedMessage *Message       `json:"edited_message"`
	ChannelPost   *Message       `json:"channel_post"`
	CallbackQuery *CallbackQuery `json:"callback_query"`
}

// Message is a chat message.
type Message struct {
	MessageID int64  `json:"message_id"`
	Chat      Chat   `json:"chat"`
	From      *User  `json:"from"`
	Text      string `json:"text"`
}

// Chat identifies the chat.
type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"` // private | group | supergroup | channel
}

// User is a Telegram user.
type User struct {
	ID       int64  `json:"id"`
	IsBot    bool   `json:"is_bot"`
	Username string `json:"username"`
}

// CallbackQuery is a keyboard button press.
type CallbackQuery struct {
	ID      string   `json:"id"`
	From    User     `json:"from"`
	Message *Message `json:"message"`
	Data    string   `json:"data"`
}

// ChatMember is the result of getChatMember.
type ChatMember struct {
	Status string `json:"status"` // creator | administrator | member | left | kicked | restricted
	User   User   `json:"user"`
}

// IsAdmin reports whether the member may administrate the chat.
func (m ChatMember) IsAdmin() bool { return m.Status == "creator" || m.Status == "administrator" }

// BotCommand is one /command hint.
type BotCommand struct {
	Command     string `json:"command"`
	Description string `json:"description"`
}

// InlineButton is one inline keyboard button.
type InlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

// InlineKeyboardMarkup is the inline keyboard.
type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineButton `json:"inline_keyboard"`
}
