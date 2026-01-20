import json
import zipfile
import hashlib
import os
import uuid
import mido

# Modified version: Keep the controller approach but use only one handler per broadcast per sprite (chaining multiple notes at the same time).
# Significantly reduce project size while stabilizing timing with frame quantization.
#
# Key points:
# - Convert all note start times to seconds → quantize to 30fps frames (qsec).
# - Create one broadcast per qsec (if there are notes in that qsec).
# - Each sprite has only one handler to receive that broadcast.
# Within the handler, sound_setvolumeto is called only once, and then the sprite's notes are executed in order with music_playNoteForBeats (simultaneous notes on the same sprite are unlikely to occur due to normal stack allocation).
# - Volume scaling is calculated every quantized seconds, ensuring a minimum value to prevent rounding to zero.
# - Maintain a 5MB fallback due to asset sharing.

MAX_ZIP_BYTES = 5 * 1024 * 1024  # 5 MB upper limit (approximate)
FRAME_RATE = 30.0  # Scratch is 30fps
MIN_VOLUME = 1  # Minimum volume (0 will not produce any sound, so set it to 1 or higher)

def generate_id():
    return str(uuid.uuid4())

def get_md5(data):
    return hashlib.md5(data).hexdigest()

def create_sb3(midi_path, output_path):
    # --- Load NIDI ---
    mid = mido.MidiFile(midi_path)
    ticks_per_beat = mid.ticks_per_beat

    # 1) Collect all events with absolute ticks
    all_events = []
    for track in mid.tracks:
        abs_tick = 0
        for msg in track:
            abs_tick += msg.time
            all_events.append((abs_tick, msg))
    all_events.sort(key=lambda x: x[0])

    # 2) Tempo event collection (same tick will be overwritten later)
    tempo_by_tick = {}
    for abs_tick, msg in all_events:
        if getattr(msg, "type", None) == 'set_tempo':
            tempo_by_tick[abs_tick] = mido.tempo2bpm(msg.tempo)
    # Default Tempo Processing
    if not tempo_by_tick:
        tempo_by_tick[0] = 120.0
    elif 0 not in tempo_by_tick:
        tempo_by_tick[0] = 120.0
    tempo_map = sorted(tempo_by_tick.items(), key=lambda x: x[0])  # (tick, bpm)

    # ticks -> beats
    def ticks_to_beats_from_ticks(ticks):
        return ticks / float(ticks_per_beat)
    tempo_map_beats = [(ticks_to_beats_from_ticks(tick), bpm) for (tick, bpm) in tempo_map]

    # --- Segmentation to stabilize beats <-> seconds conversion ---
    segments = []
    cumulative_sec = 0.0
    for i, (start_beat, bpm) in enumerate(tempo_map_beats):
        end_beat = tempo_map_beats[i+1][0] if i+1 < len(tempo_map_beats) else float('inf')
        if end_beat == float('inf'):
            start_sec = cumulative_sec
            end_sec = float('inf')
        else:
            seg_beats = end_beat - start_beat
            seg_secs = seg_beats * 60.0 / float(bpm)
            start_sec = cumulative_sec
            end_sec = cumulative_sec + seg_secs
            cumulative_sec = end_sec
        segments.append({
            'start_beat': start_beat,
            'start_sec': start_sec,
            'bpm': float(bpm),
            'end_beat': end_beat,
            'end_sec': end_sec
        })

    def beats_to_seconds(target_beat):
        # Integral calculation (segment scan)
        secs = 0.0
        for seg in segments:
            sb = seg['start_beat']
            eb = seg['end_beat']
            bpm = seg['bpm']
            if target_beat <= sb:
                break
            take = (min(target_beat, eb) - sb) if eb != float('inf') else (target_beat - sb)
            if take > 0:
                secs += take * 60.0 / bpm
            if eb == float('inf'):
                break
        return secs

    def seconds_to_beats(target_sec):
        # Inverse transformation (calculated using segments)
        for seg in segments:
            ssec = seg['start_sec']
            esec = seg['end_sec']
            bpm = seg['bpm']
            if target_sec < ssec:
                return seg['start_beat']
            if esec == float('inf') or (ssec <= target_sec <= esec):
                delta_sec = max(0.0, target_sec - ssec)
                return seg['start_beat'] + (delta_sec * bpm / 60.0)
        return segments[-1]['start_beat']

    # 3) Note extraction (order guaranteed)
    all_notes = []
    for track in mid.tracks:
        abs_tick = 0
        active_notes = {}  # note -> list of (start_tick, velocity)
        for msg in track:
            abs_tick += msg.time
            mtype = getattr(msg, "type", None)
            if mtype == 'note_on' and getattr(msg, "velocity", 0) > 0:
                active_notes.setdefault(msg.note, []).append((abs_tick, msg.velocity))
            elif mtype == 'note_off' or (mtype == 'note_on' and getattr(msg, "velocity", 0) == 0):
                if msg.note in active_notes and active_notes[msg.note]:
                    start_tick, velocity = active_notes[msg.note].pop(0)
                    duration_ticks = abs_tick - start_tick
                    if duration_ticks > 0:
                        start_beat = ticks_to_beats_from_ticks(start_tick)
                        duration_beat = ticks_to_beats_from_ticks(duration_ticks)
                        all_notes.append({
                            'note': msg.note,
                            'start_beat': start_beat,
                            'duration_beat': duration_beat,
                            'velocity': velocity
                        })
    all_notes.sort(key=lambda x: x['start_beat'])

    if not all_notes:
        # Create a project even with silent MIDI (Warning)
        print("Warning: No notes found in MIDI. Generates empty SB3.")

    # 4) Simple Polyphony Allocation（stack -> sprite）
    stacks = []
    stack_last_end_beat = []
    for n in all_notes:
        placed = False
        for i in range(len(stacks)):
            if n['start_beat'] >= stack_last_end_beat[i]:
                stacks[i].append(n)
                stack_last_end_beat[i] = n['start_beat'] + n['duration_beat']
                placed = True
                break
        if not placed:
            stacks.append([n])
            stack_last_end_beat.append(n['start_beat'] + n['duration_beat'])

    # --- Simultaneous playback scaling (calculated once using the original beat key) ---
    def key_round(b):
        return round(b, 6)

    timeline = {}
    for s_idx, stack in enumerate(stacks):
        for note in stack:
            tkey = key_round(note['start_beat'])
            vel = note.get('velocity', 100)
            raw_vol = int(round((vel / 127.0) * 100))
            raw_vol = max(MIN_VOLUME, raw_vol)
            timeline.setdefault(tkey, []).append((s_idx, raw_vol))

    scaled_by_beat = {}
    for tkey, entries in timeline.items():
        total = sum(v / 100.0 for (_, v) in entries)
        if total <= 1.0:
            for s_idx, v in entries:
                scaled_by_beat[(tkey, s_idx)] = v
        else:
            factor = 1.0 / total
            for s_idx, v in entries:
                sv = int(round(v * factor))
                scaled_by_beat[(tkey, s_idx)] = max(MIN_VOLUME, sv)

    # --- Convert each note to seconds and frame quantize (qsec) to create events_by_sec ---
    events_by_sec = {}  # qsec -> {'tempo': bpm_or_None, 'notes': [ {...} ]}

    # tempo events (quantized)
    for seg in segments:
        b = seg['start_beat']
        sec = beats_to_seconds(b)
        frames = int(round(sec * FRAME_RATE))
        qsec = frames / FRAME_RATE
        events_by_sec.setdefault(qsec, {'tempo': None, 'notes': []})
        events_by_sec[qsec]['tempo'] = seg['bpm']

    # notes -> quantized seconds
    for s_idx, stack in enumerate(stacks):
        for note in stack:
            start_beat = note['start_beat']
            sec = beats_to_seconds(start_beat)
            frames = int(round(sec * FRAME_RATE))
            qsec = frames / FRAME_RATE
            beat_key = key_round(start_beat)
            pref_vol = scaled_by_beat.get((beat_key, s_idx),
                                          max(MIN_VOLUME, int(round((note.get('velocity',100)/127.0)*100))))
            events_by_sec.setdefault(qsec, {'tempo': None, 'notes': []})
            events_by_sec[qsec]['notes'].append({
                'sprite_idx': s_idx,
                'note': note['note'],
                'duration_beat': note['duration_beat'],
                'vol_pref': pref_vol,
                'orig_start_beat': start_beat
            })

    # Scaling: Readjust by looking at the total over the same qsec (this is the actual volume used)
    for qsec, entry in events_by_sec.items():
        notes = entry['notes']
        if not notes:
            continue
        total = sum(n['vol_pref'] / 100.0 for n in notes)
        if total <= 1.0:
            for n in notes:
                v = int(round(n['vol_pref']))
                n['vol'] = max(MIN_VOLUME, min(100, v)) if v > 0 else 0
        else:
            factor = 1.0 / total
            for n in notes:
                v = int(round(n['vol_pref'] * factor))
                n['vol'] = max(MIN_VOLUME, min(100, v)) if n['vol_pref'] > 0 else 0

    sorted_secs = sorted(events_by_sec.keys())

    # --- Controller block generation (1 broadcast per qsec) ---
    broadcast_base = "note_event"
    controller_blocks = {}
    start_id = generate_id()
    controller_blocks[start_id] = {
        "opcode": "event_whenflagclicked", "next": None, "parent": None,
        "inputs": {}, "fields": {}, "shadow": False, "topLevel": True, "x": 0, "y": 0
    }

    initial_bpm = float(tempo_map_beats[0][1]) if tempo_map_beats else 120.0
    tempo_block_id = generate_id()
    controller_blocks[start_id]['next'] = tempo_block_id
    controller_blocks[tempo_block_id] = {
        "opcode": "music_setTempo", "next": None, "parent": start_id,
        "inputs": {"TEMPO": [1, [4, str(round(initial_bpm, 6))]]},
        "fields": {}, "shadow": False, "topLevel": False
    }

    prev_id = tempo_block_id
    last_time = 0.0
    event_idx = 0
    # if there are no events, we keep the chain minimal (only tempo set)
    if sorted_secs:
        # if first qsec > 0, wait to first event
        if sorted_secs[0] > 0:
            wait_id = generate_id()
            controller_blocks[wait_id] = {
                "opcode": "control_wait", "next": None, "parent": prev_id,
                "inputs": {"DURATION": [1, [4, str(round(sorted_secs[0], 6))]]},
                "fields": {}, "shadow": False, "topLevel": False
            }
            controller_blocks[prev_id]["next"] = wait_id
            prev_id = wait_id
            last_time = sorted_secs[0]
        for qsec in sorted_secs:
            if qsec > last_time:
                wait_id = generate_id()
                controller_blocks[wait_id] = {
                    "opcode": "control_wait", "next": None, "parent": prev_id,
                    "inputs": {"DURATION": [1, [4, str(round(qsec - last_time, 6))]]},
                    "fields": {}, "shadow": False, "topLevel": False
                }
                controller_blocks[prev_id]["next"] = wait_id
                prev_id = wait_id
                last_time = qsec
            # tempo change at this qsec?
            tempo_here = events_by_sec[qsec].get('tempo', None)
            if tempo_here is not None and len(tempo_map_beats) > 1:
                settempo_id = generate_id()
                controller_blocks[settempo_id] = {
                    "opcode": "music_setTempo", "next": None, "parent": prev_id,
                    "inputs": {"TEMPO": [1, [4, str(round(float(tempo_here), 6))]]},
                    "fields": {}, "shadow": False, "topLevel": False
                }
                controller_blocks[prev_id]["next"] = settempo_id
                prev_id = settempo_id
            # broadcast if notes exist
            if events_by_sec[qsec]['notes']:
                bname = f"{broadcast_base}_{event_idx}"
                bid = generate_id()
                controller_blocks[prev_id]["next"] = bid
                controller_blocks[bid] = {
                    "opcode": "event_broadcast", "next": None, "parent": prev_id,
                    "inputs": {"BROADCAST_INPUT": [1, [11, bname, bid]]},
                    "fields": {}, "shadow": False, "topLevel": False
                }
                events_by_sec[qsec]['broadcast'] = (bname, bid)
                prev_id = bid
                event_idx += 1

    # --- Sprite side: Generate one handler per sprite for each broadcast (and chain multiple notes within it) ---
    sprite_entries = []
    asset_files = {}
    dummy_svg_base = '<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"></svg>'.encode()
    dummy_wav_base = b'RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x01\x00\x80\xbb\x00\x00\x00\x77\x01\x00\x02\x00\x10\x00data\x00\x00\x00\x00'
    stage_back_md5ext = "cd21514d0531fdffb22204e0ec5ed84a.svg"
    stage_pop_md5ext = "83a9787d4cb6f3b7632b4ddfebf74367.wav"
    asset_files.setdefault(stage_back_md5ext, dummy_svg_base)
    asset_files.setdefault(stage_pop_md5ext, dummy_wav_base)

    # controller target (holds the controller blocks)
    controller_costume_md5hex = get_md5(dummy_svg_base)
    controller_costume_md5ext = controller_costume_md5hex + ".svg"
    controller_sound_md5hex = get_md5(dummy_wav_base)
    controller_sound_md5ext = controller_sound_md5hex + ".wav"
    asset_files.setdefault(controller_costume_md5ext, dummy_svg_base)
    asset_files.setdefault(controller_sound_md5ext, dummy_wav_base)
    controller_target = {
        "isStage": False, "name": "Controller", "variables": {}, "lists": {}, "broadcasts": {},
        "blocks": controller_blocks, "comments": {}, "currentCostume": 0,
        "costumes": [{"name": "costume1", "bitmapResolution": 1, "dataFormat": "svg",
                      "assetId": controller_costume_md5hex, "md5ext": controller_costume_md5ext,
                      "rotationCenterX": 48, "rotationCenterY": 50}],
        "sounds": [{"name": "silent", "assetId": controller_sound_md5hex, "dataFormat": "wav",
                    "format": "", "rate": 48000, "sampleCount": 1, "md5ext": controller_sound_md5ext}],
        "volume": 100, "layerOrder": 1, "visible": True, "x": 0, "y": 0, "size": 100,
        "direction": 90, "draggable": False, "rotationStyle": "all around"
    }
    sprite_entries.append(controller_target)

    # Prepare a mapping qsec -> per-sprite list of notes (to simplify sprite handler generation)
    per_qsec_sprite_notes = {}
    for qsec, node in events_by_sec.items():
        if 'broadcast' not in node:
            continue
        # group notes by sprite index
        grouping = {}
        for n in node['notes']:
            s = n['sprite_idx']
            grouping.setdefault(s, []).append(n)
        per_qsec_sprite_notes[qsec] = {
            'broadcast': node['broadcast'],
            'by_sprite': grouping
        }

    # For each sprite create handlers for broadcasts that include notes for that sprite
    for s_idx, stack_notes in enumerate(stacks):
        sprite_name = f"Voice_{s_idx+1}"
        sprite_blocks = {}
        # iterate qsec where this sprite has notes
        for qsec in sorted(per_qsec_sprite_notes.keys()):
            info = per_qsec_sprite_notes[qsec]
            bname, bid = info['broadcast']
            notes_for_sprite = info['by_sprite'].get(s_idx, [])
            if not notes_for_sprite:
                continue
            # Create a single when-receive handler for this sprite + broadcast
            when_id = generate_id()
            sprite_blocks[when_id] = {
                "opcode": "event_whenbroadcastreceived",
                "next": None,
                "parent": None,
                "inputs": {},
                "fields": {"BROADCAST_OPTION": [bname, bid]},
                "shadow": False,
                "topLevel": True,
                "x": 0, "y": 0
            }
            prev_id = when_id
            # Decide a single sprite-level volume for this handler.
            # Use the maximum of vols among notes for the sprite at this qsec (conservative)
            vols = [int(n['vol']) for n in notes_for_sprite]
            if vols:
                vol_value = max(min(100, v) for v in vols)
            else:
                vol_value = MIN_VOLUME
            # Always set volume at start of handler (sprite-local)
            vol_id = generate_id()
            sprite_blocks[vol_id] = {
                "opcode": "sound_setvolumeto",
                "next": None,
                "parent": prev_id,
                "inputs": {"VOLUME": [1, [4, str(int(vol_value))]]},
                "fields": {},
                "shadow": False,
                "topLevel": False
            }
            sprite_blocks[prev_id]["next"] = vol_id
            prev_id = vol_id
            # Chain the play blocks for all notes_for_sprite
            for ev in notes_for_sprite:
                play_id = generate_id()
                note_shadow = generate_id()
                duration_beats = max(0.01, ev.get('duration_beat', 0.01))
                sprite_blocks[play_id] = {
                    "opcode": "music_playNoteForBeats",
                    "next": None,
                    "parent": prev_id,
                    "inputs": {"NOTE": [1, note_shadow], "BEATS": [1, [4, str(round(duration_beats, 6))]]},
                    "fields": {},
                    "shadow": False,
                    "topLevel": False
                }
                sprite_blocks[note_shadow] = {
                    "opcode": "note",
                    "next": None,
                    "parent": play_id,
                    "inputs": {},
                    "fields": {"NOTE": [str(ev['note']), None]},
                    "shadow": True,
                    "topLevel": False
                }
                sprite_blocks[prev_id]["next"] = play_id
                prev_id = play_id
        # sprite assets
        costume_data = dummy_svg_base
        sound_data = dummy_wav_base
        costume_md5hex = get_md5(costume_data + sprite_name.encode())
        costume_md5ext = costume_md5hex + ".svg"
        sound_md5hex = get_md5(sound_data + sprite_name.encode())
        sound_md5ext = sound_md5hex + ".wav"
        asset_files.setdefault(costume_md5ext, costume_data)
        asset_files.setdefault(sound_md5ext, sound_data)

        sprite_target = {
            "isStage": False, "name": sprite_name, "variables": {}, "lists": {}, "broadcasts": {},
            "blocks": sprite_blocks, "comments": {}, "currentCostume": 0,
            "costumes": [{"name": "costume1", "bitmapResolution": 1, "dataFormat": "svg",
                          "assetId": costume_md5hex, "md5ext": costume_md5ext, "rotationCenterX": 48, "rotationCenterY": 50}],
            "sounds": [{"name": "sound", "assetId": sound_md5hex, "dataFormat": "wav",
                        "format": "", "rate": 48000, "sampleCount": 1, "md5ext": sound_md5ext}],
            "volume": 100, "layerOrder": 1 + s_idx + 1, "visible": True, "x": 0, "y": 0,
            "size": 100, "direction": 90, "draggable": False, "rotationStyle": "all around"
        }
        sprite_entries.append(sprite_target)

    # --- Assembling the Project ---
    project_targets = [
        {
            "isStage": True, "name": "Stage",
            "variables": {"`jEk@4|i[#Fk?(8x)AV.-my variable": ["変数", 0]},
            "lists": {}, "broadcasts": {}, "blocks": {}, "comments": {},
            "currentCostume": 0, "costumes": [{"name": "backdrop1", "dataFormat": "svg",
                                              "assetId": stage_back_md5ext[:-4], "md5ext": stage_back_md5ext,
                                              "rotationCenterX": 240, "rotationCenterY": 180}],
            "sounds": [{"name": "pop", "assetId": stage_pop_md5ext[:-4], "dataFormat": "wav",
                        "format": "", "rate": 48000, "sampleCount": 1123, "md5ext": stage_pop_md5ext}],
            "volume": 100, "layerOrder": 0, "tempo": round(initial_bpm, 6) if 'initial_bpm' in locals() else 120.0,
            "videoTransparency": 50, "videoState": "on", "textToSpeechLanguage": None
        }
    ]
    project_targets.extend(sprite_entries)
    project = {"targets": project_targets, "monitors": [], "extensions": ["music"],
               "meta": {"semver": "3.0.0", "vm": "12.1.3", "agent": "Manus MIDI to Scratch Converter"}}

    # compact JSON
    project_json_bytes = json.dumps(project, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    estimated_size = len(project_json_bytes) + sum(len(b) for b in asset_files.values())

    reuse_assets = False
    if estimated_size > MAX_ZIP_BYTES:
        reuse_assets = True
        common_costume_md5hex = get_md5(dummy_svg_base)
        common_costume_md5ext = common_costume_md5hex + ".svg"
        common_sound_md5hex = get_md5(dummy_wav_base)
        common_sound_md5ext = common_sound_md5hex + ".wav"
        asset_files = {stage_back_md5ext: dummy_svg_base, stage_pop_md5ext: dummy_wav_base,
                       common_costume_md5ext: dummy_svg_base, common_sound_md5ext: dummy_wav_base}
        for t in project_targets[1:]:
            t["costumes"][0]["assetId"] = common_costume_md5hex
            t["costumes"][0]["md5ext"] = common_costume_md5ext
            t["sounds"][0]["assetId"] = common_sound_md5hex
            t["sounds"][0]["md5ext"] = common_sound_md5ext
        project_json_bytes = json.dumps(project, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
        estimated_size = len(project_json_bytes) + sum(len(b) for b in asset_files.values())

    if estimated_size > MAX_ZIP_BYTES:
        print(f"WARNING: Estimated size is {estimated_size} bytes, which is over 5MB (even with automatic sharing).")
    else:
        print(f"Estimated sb3 size: {estimated_size} bytes")

    # Export to zip
    with zipfile.ZipFile(output_path, 'w') as zipf:
        zipf.writestr('project.json', project_json_bytes)
        for md5ext, data in asset_files.items():
            zipf.writestr(md5ext, data)

    print("SB3 creation completed:", output_path)
    if reuse_assets:
        print("Created in asset sharing mode (to save size).")

midi_path = "/mnt/data/midi.mid"
output_path = "/mnt/data/output.sb3"

try:
    create_sb3(midi_path, output_path)
    print(f"success: {output_path} created.")
except Exception as e:
    print(f"Error: {e}")
