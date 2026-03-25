import { useState, useRef, useEffect } from 'react';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

const INITIAL_MESSAGES: Message[] = [
  {
    id: '1',
    role: 'assistant',
    content: 'Hello! I\'m your Government Data Pipeline Assistant. I can help you explore data, understand metrics, and answer questions about the ingested documents. What would you like to know?',
    timestamp: new Date(),
  },
];

const SAMPLE_RESPONSES: Record<string, string> = {
  'dis': 'The Data Ingestion Score (DIS) is a composite metric that measures pipeline health. It combines Quality (40%), Efficiency (30%), and Execution Success (30%). A higher DIS indicates better overall pipeline performance.',
  'quality': 'Quality Score measures the accuracy and completeness of document parsing. It considers factors like table extraction accuracy, section identification, and content structure preservation.',
  'efficiency': 'Efficiency Score measures processing speed relative to a target time of 5 minutes. Instant processing scores 100, at-target scores 75, and the score decreases linearly for longer processing times.',
  'workflow': 'Workflows define the complete data processing pipeline for an asset, including acquisition, parsing, enrichment, and relationship extraction steps.',
  'entity': 'Entities are key concepts extracted from documents, such as organizations, people, dates, and immigration categories. They form the basis of the knowledge graph.',
  'relationship': 'Relationships connect entities across documents, enabling cross-document analysis and creating a knowledge graph of government data.',
};

export default function ChatBot() {
  const [messages, setMessages] = useState<Message[]>(INITIAL_MESSAGES);
  const [input, setInput] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const generateResponse = (userMessage: string): string => {
    const lowerMessage = userMessage.toLowerCase();

    // Check for keyword matches
    for (const [keyword, response] of Object.entries(SAMPLE_RESPONSES)) {
      if (lowerMessage.includes(keyword)) {
        return response;
      }
    }

    // Default responses
    if (lowerMessage.includes('hello') || lowerMessage.includes('hi')) {
      return 'Hello! How can I help you explore the government data pipeline today?';
    }

    if (lowerMessage.includes('help')) {
      return 'I can help you with:\n• Understanding DIS metrics and scores\n• Explaining workflow processes\n• Describing entity relationships\n• Answering questions about document quality\n\nJust ask me anything about the pipeline!';
    }

    if (lowerMessage.includes('thank')) {
      return 'You\'re welcome! Let me know if you have any other questions about the data pipeline.';
    }

    return 'I understand you\'re asking about "' + userMessage.slice(0, 50) + '". This is a demo chatbot - in production, I would connect to an LLM to provide intelligent responses about your government data pipeline. Try asking about DIS, quality, efficiency, workflows, entities, or relationships!';
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim()) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsTyping(true);

    // Simulate AI response delay
    setTimeout(() => {
      const response: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: generateResponse(userMessage.content),
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, response]);
      setIsTyping(false);
    }, 800 + Math.random() * 800);
  };

  return (
    <div className="flex flex-col h-full bg-slate-800 rounded-lg overflow-hidden">
      {/* Chat Header */}
      <div className="px-4 py-3 bg-slate-700 border-b border-slate-600">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse" />
          <span className="text-sm font-medium text-white">Pipeline Assistant</span>
        </div>
        <p className="text-xs text-slate-400 mt-0.5">Ask questions about your data</p>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4 min-h-[200px] max-h-[300px]">
        {messages.map((message) => (
          <div
            key={message.id}
            className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
          >
            <div
              className={`max-w-[85%] rounded-lg px-3 py-2 ${
                message.role === 'user'
                  ? 'bg-blue-600 text-white'
                  : 'bg-slate-700 text-slate-100'
              }`}
            >
              <p className="text-sm whitespace-pre-wrap">{message.content}</p>
              <p className="text-[10px] mt-1 opacity-60">
                {message.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </p>
            </div>
          </div>
        ))}
        {isTyping && (
          <div className="flex justify-start">
            <div className="bg-slate-700 text-slate-100 rounded-lg px-3 py-2">
              <div className="flex gap-1">
                <span className="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                <span className="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                <span className="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <form onSubmit={handleSubmit} className="p-3 border-t border-slate-600">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about DIS, workflows, entities..."
            className="flex-1 bg-slate-700 text-white text-sm rounded-lg px-3 py-2 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <button
            type="submit"
            disabled={!input.trim() || isTyping}
            className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            Send
          </button>
        </div>
      </form>
    </div>
  );
}
