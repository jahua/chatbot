export interface ChatMessage {
  role: 'user' | 'assistant' | 'error';
  content: string;
  timestamp: string;
  sql_query?: string;
  visualization?: string;
  debug_info?: any;
}

export interface StreamingChatChunk {
  type: 'start' | 'content' | 'content_start' | 'sql_query' | 'visualization' | 'debug_info' | 'end' | 'error';
  message_id?: string;
  content?: string;
  sql_query?: string;
  visualization?: string;
  debug_info?: any;
  error?: string;
} 