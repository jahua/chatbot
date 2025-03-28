export interface ChatMessage {
  role: 'user' | 'assistant' | 'error';
  content: string;
  timestamp: string;
  sql_query?: string;
} 