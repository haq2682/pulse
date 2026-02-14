# Pipeline UI States - Visual Reference

## State 1: No Pipeline (Start Analysis)

```
┌─────────────────────────────────────────────────────────┐
│                     Dashboard                            │
├─────────────────────────────────────────────────────────┤
│  [Add Business]  [Select Business ▼]                    │
├─────────────────────────────────────────────────────────┤
│                                                          │
│                     📊 (large icon)                      │
│                                                          │
│              Ready to Analyze Your Data                  │
│                                                          │
│    Start the analysis pipeline to process your data     │
│    through cleaning, transformation, analysis, and      │
│    machine learning phases.                             │
│                                                          │
│                  [▶ Start Analysis]                      │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## State 2: Pipeline Running

```
┌─────────────────────────────────────────────────────────┐
│                     Dashboard                            │
├─────────────────────────────────────────────────────────┤
│  [Add Business]  [Select Business ▼]                    │
├─────────────────────────────────────────────────────────┤
│                                                          │
│                      ╔════════╗                          │
│                      ║        ║                          │
│                      ║  45%   ║  (Rotating Knob)         │
│                      ║        ║                          │
│                      ╚════════╝                          │
│                                                          │
│              Processing Your Data                        │
│                                                          │
│         Transforming & Aggregating Data                  │
│                   ● Connected                            │
│                                                          │
│              Pipeline Phases:                            │
│              ✓ Cleaning Data (0-25%)                     │
│              ○ Transforming & Aggregating (25-55%)       │
│              ○ Analyzing Data (55-85%)                   │
│              ○ Running ML Predictions (85-100%)          │
│                                                          │
│                [✕ Cancel Pipeline]                       │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## State 3: Pipeline Completed

```
┌─────────────────────────────────────────────────────────┐
│                     Dashboard                            │
├─────────────────────────────────────────────────────────┤
│  [Add Business]  [Select Business ▼]                    │
├─────────────────────────────────────────────────────────┤
│                                                          │
│                      ╔════════╗                          │
│                      ║        ║                          │
│                      ║ 100%   ║  (Green Knob)            │
│                      ║        ║                          │
│                      ╚════════╝                          │
│                                                          │
│                     ✓ (check icon)                       │
│                                                          │
│                Analysis Complete!                        │
│                                                          │
│      Your data has been successfully processed           │
│          and is ready for visualization.                 │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## State 4: Pipeline Failed

```
┌─────────────────────────────────────────────────────────┐
│                     Dashboard                            │
├─────────────────────────────────────────────────────────┤
│  [Add Business]  [Select Business ▼]                    │
├─────────────────────────────────────────────────────────┤
│                                                          │
│                      ╔════════╗                          │
│                      ║        ║                          │
│                      ║  55%   ║  (Red Knob)              │
│                      ║        ║                          │
│                      ╚════════╝                          │
│                                                          │
│                     ⚠ (warning icon)                     │
│                                                          │
│                  Analysis Failed                         │
│                                                          │
│    An error occurred while processing your data.         │
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Pipeline failed during analysis phase           │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│           Failed at: analysis phase                      │
│                                                          │
│            [↻ Retry from analysis]                       │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## Component Structure

```
InlinePipelineProgress
├── No Pipeline State
│   ├── Icon (chart-line)
│   ├── Heading: "Ready to Analyze Your Data"
│   ├── Description text
│   └── [Start Analysis] button
│
├── Running State
│   ├── Knob (0-100%, green)
│   ├── Heading: "Processing Your Data"
│   ├── Current step text
│   ├── Connection status
│   ├── Phase checklist
│   │   ├── ✓/○ Cleaning (0-25%)
│   │   ├── ✓/○ Transformation (25-55%)
│   │   ├── ✓/○ Analysis (55-85%)
│   │   └── ✓/○ ML (85-100%)
│   └── [Cancel Pipeline] button
│
├── Completed State
│   ├── Knob (100%, green)
│   ├── Success icon
│   ├── Heading: "Analysis Complete!"
│   └── Success message
│
└── Failed State
    ├── Knob (progress%, red)
    ├── Warning icon
    ├── Heading: "Analysis Failed"
    ├── Error message box
    ├── Failed phase indicator
    └── [Retry from {phase}] button
```

## Key Differences from Previous Implementation

### Before (Modal Popup)
```
Dashboard → [Dialog opens on top]
            ├── Blocks entire dashboard
            ├── Must be dismissed
            └── Separate from content flow
```

### After (Inline Display)
```
Dashboard → Content area
            ├── Replaces placeholder text
            ├── Integrated in page flow
            └── Always visible when business selected
```

## Interaction Flow

```
1. User visits dashboard
   └── No business selected
       └── Shows: "Add business" message

2. User selects business
   └── Business has no pipeline
       └── Shows: "Start Analysis" button

3. User clicks "Start Analysis"
   └── POST /pipeline/start
       └── Shows: Running state with knob

4. Pipeline progresses
   └── WebSocket updates
       └── Knob animates 0→25→55→85→100%
           └── Phase checkmarks update

5a. Success path
    └── Shows: Completed state (100%, green)

5b. Failure path
    └── Shows: Failed state
        └── User clicks "Retry from {phase}"
            └── POST /pipeline/retry
                └── Resumes from failed phase
```

## Technical Implementation Notes

### State Determination Logic
```javascript
const hasNoPipeline = !pipelineStatus || pipelineStatus.status === 'cancelled';
const isRunning = pipelineStatus?.status === 'running';
const isCompleted = pipelineStatus?.status === 'completed';
const isFailed = pipelineStatus?.status === 'failed';
```

### Phase Progress Calculation
```javascript
progress >= 0    → Cleaning started
progress >= 25   → Transformation started
progress >= 55   → Analysis started
progress >= 85   → ML started
progress === 100 → All complete
```

### Button State Logic
```javascript
if (hasNoPipeline)  → Show "Start Analysis"
if (isRunning)      → Show "Cancel Pipeline"
if (isFailed)       → Show "Retry from {failed_phase}"
if (isCompleted)    → No button (success state)
```

## Color Scheme

- **Running**: `var(--color-g2)` (brand green)
- **Completed**: `#22c55e` (success green)
- **Failed**: `#ef4444` (error red)
- **Text**: Gray scale for hierarchy
- **Borders**: Light gray for containers

## Responsive Design

The component uses Tailwind classes for responsiveness:
- `min-h-[60vh]` - Minimum height 60% viewport
- `max-w-2xl` - Maximum width constraint
- `p-8` - Padding for breathing room
- `text-center` - Centered layout
- Icons scale with text size

## Accessibility

- Clear visual hierarchy
- Color + icons (not color alone)
- Descriptive button labels
- Status messages for screen readers
- Keyboard accessible buttons
