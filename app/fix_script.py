#!/usr/bin/env python

def fix_chat_service():
    fixed_lines = []
    skip_mode = False
    
    with open('services/chat_service.py', 'r') as f:
        lines = f.readlines()
    
    for i, line in enumerate(lines):
        line_num = i + 1
        
        # Skip the problematic else statement and indent the following lines
        if line_num == 566 and 'else:' in line:
            skip_mode = True
            # Insert a comment explaining the fix
            fixed_lines.append('                    # No results case - removed invalid else statement\n')
            continue
            
        # Re-add the response assignment with proper indentation
        if line_num == 567 and skip_mode and "response =" in line:
            fixed_lines.append('                    response = "I couldn\'t find spending data by industry."\n')
            continue
            
        fixed_lines.append(line)
    
    # Write the fixed file
    with open('services/chat_service.py.new', 'w') as f:
        f.writelines(fixed_lines)
    
    print("Fixed file created at services/chat_service.py.new")

if __name__ == "__main__":
    fix_chat_service() 