import { NextRequest } from 'next/server';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    // Forward the request to the backend API streaming endpoint
    const response = await fetch('http://localhost:8081/chat/stream', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    
    if (!response.ok) {
      throw new Error(`Backend API returned ${response.status}`);
    }
    
    // Create a new response that forwards the streaming data
    return new Response(response.body, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      }
    });
  } catch (error) {
    console.error('Error in streaming chat API route:', error);
    
    // Return an error message in the stream format
    return new Response(
      JSON.stringify({
        type: 'error',
        error: error instanceof Error ? error.message : 'Unknown error'
      }) + '\n',
      {
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive'
        },
        status: 500
      }
    );
  }
} 