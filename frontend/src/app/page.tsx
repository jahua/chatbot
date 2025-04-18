'use client';

import { useState, useEffect, useRef } from 'react';
import { ChatMessage } from '@/types/chat';
import Script from 'next/script';

// Import Plotly if using client-side rendering
const PlotlyComponent = ({ figure }: { figure: any }) => {
  const divRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (typeof window !== 'undefined' && divRef.current) {
      // This needs to be a dynamic import since Plotly is not SSR-friendly
      import('plotly.js-dist').then((Plotly) => {
        Plotly.newPlot(divRef.current!, figure.data, figure.layout);
      });
    }
  }, [figure]);

  return <div ref={divRef} style={{ width: '100%', height: '400px' }} />;
};

export default function Home() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const [streamingMessage, setStreamingMessage] = useState<ChatMessage | null>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const renderVisualization = (visualizationData: string) => {
    try {
      // First check if it looks like base64 data
      if (visualizationData.match(/^[A-Za-z0-9+/=]+$/)) {
        return (
          <div className="mt-3 p-2 bg-white rounded-lg border border-gray-200">
            <img 
              src={`data:image/png;base64,${visualizationData}`} 
              alt="Visualization" 
              className="max-w-full" 
            />
          </div>
        );
      }
      
      // Try to parse as JSON (for Plotly figures)
      try {
        const parsed = JSON.parse(visualizationData);
        
        if (parsed.figure && parsed.type === 'bar') {
          return (
            <div className="mt-3 p-2 bg-white rounded-lg border border-gray-200">
              <PlotlyComponent figure={parsed.figure} />
            </div>
          );
        }
      } catch (jsonError) {
        console.error("Error parsing visualization JSON:", jsonError);
      }
      
      return (
        <div className="mt-3 p-2 bg-white rounded-lg border border-gray-200">
          <p className="text-sm text-gray-500">Unknown visualization format</p>
          <details>
            <summary className="text-xs text-gray-400 cursor-pointer">Details</summary>
            <pre className="mt-1 text-xs overflow-auto max-h-40">
              {visualizationData.substring(0, 100)}...
            </pre>
          </details>
        </div>
      );
    } catch (e) {
      // If all else fails, show error
      return (
        <div className="mt-3 p-2 bg-red-50 rounded-lg border border-red-200">
          <p className="text-sm text-red-500">Error displaying visualization</p>
          <p className="text-xs text-gray-500">{String(e)}</p>
        </div>
      );
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || loading) return;

    const userMessage: ChatMessage = {
      role: 'user',
      content: input,
      timestamp: new Date().toISOString()
    };

    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setLoading(true);

    try {
      // Create an empty assistant message for streaming
      const initialAssistantMessage: ChatMessage = {
        role: 'assistant',
        content: '',
        timestamp: new Date().toISOString()
      };
      
      // Set it as the streaming message
      setStreamingMessage(initialAssistantMessage);

      // Use the streaming endpoint
      const response = await fetch('/api/chat/stream', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ message: input }),
      });

      if (!response.ok) {
        throw new Error(`Server responded with ${response.status}: ${response.statusText}`);
      }

      // Get reader for the stream
      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error('Failed to get stream reader');
      }

      let messageId = '';
      
      // Process the stream
      while (true) {
        const { done, value } = await reader.read();
        
        if (done) {
          break;
        }
        
        // Convert the chunk to text
        const chunk = new TextDecoder().decode(value);
        const lines = chunk.split('\n').filter(line => line.trim() !== '');
        
        for (const line of lines) {
          try {
            const data = JSON.parse(line);
            
            // Update the streaming message based on the chunk type
            setStreamingMessage(prev => {
              if (!prev) return prev;
              
              const updated = { ...prev };
              
              // Handle different chunk types
              if (data.type === 'start' && data.message_id) {
                messageId = data.message_id;
              }
              else if (data.type === 'content') {
                updated.content = (updated.content || '') + data.content;
              }
              else if (data.type === 'sql_query') {
                updated.sql_query = data.sql_query;
              }
              else if (data.type === 'visualization') {
                updated.visualization = data.visualization;
              }
              else if (data.type === 'debug_info') {
                updated.debug_info = data.debug_info;
              }
              else if (data.type === 'error') {
                updated.content = `Error: ${data.error}`;
                updated.role = 'error';
              }
              
              return updated;
            });
          } catch (error) {
            console.error('Error parsing chunk:', error, line);
          }
        }
      }
      
      // When streaming is complete, move the streaming message to the regular messages
      setMessages(prev => {
        if (streamingMessage) {
          return [...prev, streamingMessage];
        }
        return prev;
      });
      
      // Clear the streaming message
      setStreamingMessage(null);
    } catch (error) {
      console.error('Error:', error);
      const errorMessage: ChatMessage = {
        role: 'error',
        content: 'Sorry, there was an error processing your message.',
        timestamp: new Date().toISOString()
      };
      setMessages(prev => [...prev, errorMessage]);
      setStreamingMessage(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="flex min-h-screen flex-col items-center p-4 bg-gray-100">
      {/* Load Plotly.js from CDN */}
      <Script src="https://cdn.plot.ly/plotly-2.24.1.min.js" strategy="lazyOnload" />
      
      <div className="w-full max-w-4xl bg-white rounded-lg shadow-lg overflow-hidden">
        <div className="p-4 border-b">
          <h1 className="text-2xl font-bold text-gray-800">Tourism Data Chat</h1>
        </div>
        
        <div className="h-[600px] overflow-y-auto p-4 space-y-4">
          {messages.map((message, index) => (
            <div
              key={index}
              className={`flex ${
                message.role === 'user' ? 'justify-end' : 'justify-start'
              }`}
            >
              <div
                className={`max-w-[80%] rounded-lg p-3 ${
                  message.role === 'user'
                    ? 'bg-blue-500 text-white'
                    : message.role === 'error'
                    ? 'bg-red-100 text-red-800'
                    : 'bg-gray-100 text-gray-800'
                }`}
              >
                <p className="whitespace-pre-wrap">{message.content}</p>
                
                {message.visualization && message.role === 'assistant' && (
                  renderVisualization(message.visualization)
                )}
                
                {message.sql_query && (
                  <div className="mt-2 p-2 bg-gray-800 text-white rounded text-sm font-mono">
                    <p className="text-xs text-gray-400 mb-1">SQL Query:</p>
                    <p className="whitespace-pre-wrap">{message.sql_query}</p>
                  </div>
                )}
                
                {message.debug_info && (
                  <div className="mt-2 p-2 bg-gray-700 text-white rounded text-sm">
                    <details>
                      <summary className="text-xs text-gray-400 cursor-pointer">Debug Info</summary>
                      <pre className="mt-1 text-xs overflow-auto max-h-40">
                        {JSON.stringify(message.debug_info, null, 2)}
                      </pre>
                    </details>
                  </div>
                )}
              </div>
            </div>
          ))}
          
          {/* Show streaming message if available */}
          {streamingMessage && (
            <div className="flex justify-start">
              <div className={`max-w-[80%] rounded-lg p-3 ${
                streamingMessage.role === 'error'
                ? 'bg-red-100 text-red-800'
                : 'bg-gray-100 text-gray-800'
              }`}>
                <p className="whitespace-pre-wrap">{streamingMessage.content}</p>
                
                {streamingMessage.visualization && streamingMessage.role === 'assistant' && (
                  renderVisualization(streamingMessage.visualization)
                )}
                
                {streamingMessage.sql_query && (
                  <div className="mt-2 p-2 bg-gray-800 text-white rounded text-sm font-mono">
                    <p className="text-xs text-gray-400 mb-1">SQL Query:</p>
                    <p className="whitespace-pre-wrap">{streamingMessage.sql_query}</p>
                  </div>
                )}
                
                {streamingMessage.debug_info && (
                  <div className="mt-2 p-2 bg-gray-700 text-white rounded text-sm">
                    <details>
                      <summary className="text-xs text-gray-400 cursor-pointer">Debug Info</summary>
                      <pre className="mt-1 text-xs overflow-auto max-h-40">
                        {JSON.stringify(streamingMessage.debug_info, null, 2)}
                      </pre>
                    </details>
                  </div>
                )}
                
                {/* Show typing indicator for streaming */}
                <span className="inline-block mt-2 h-2 w-2 bg-gray-500 rounded-full animate-pulse"></span>
                <span className="inline-block ml-1 mt-2 h-2 w-2 bg-gray-500 rounded-full animate-pulse" style={{ animationDelay: '0.2s' }}></span>
                <span className="inline-block ml-1 mt-2 h-2 w-2 bg-gray-500 rounded-full animate-pulse" style={{ animationDelay: '0.4s' }}></span>
              </div>
            </div>
          )}
          
          <div ref={messagesEndRef} />
        </div>

        <form onSubmit={handleSubmit} className="p-4 border-t">
          <div className="flex space-x-2">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Ask about tourism data..."
              className="flex-1 p-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              disabled={loading}
            />
            <button
              type="submit"
              disabled={loading}
              className={`px-4 py-2 rounded-lg text-white ${
                loading
                  ? 'bg-gray-400 cursor-not-allowed'
                  : 'bg-blue-500 hover:bg-blue-600'
              }`}
            >
              {loading ? 'Sending...' : 'Send'}
            </button>
          </div>
        </form>
      </div>
    </main>
  );
} 