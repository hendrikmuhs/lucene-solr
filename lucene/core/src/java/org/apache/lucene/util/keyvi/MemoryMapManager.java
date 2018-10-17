package org.apache.lucene.util.keyvi;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.DataOutput;

import java.nio.MappedByteBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class MemoryMapManager implements Closeable {

	private int chunkSize;
	private Path directory;
	private String filenamePattern;
	private List<MappedByteBuffer> mappings = new ArrayList<MappedByteBuffer>();
	private List<RandomAccessFile> files = new ArrayList<RandomAccessFile>();
	private int tail = 0;

	public MemoryMapManager(int chunkSize, Path directory, String filenamePattern) {
		this.chunkSize = chunkSize;
		this.directory = directory;
		this.filenamePattern = filenamePattern;
	}
	
	@Override
	public void close() throws IOException {
		mappings.clear();
		for (RandomAccessFile f : files) {
			f.close();
		}
		files.clear();
	}

	/*
	 * Using GetAdress to read multiple bytes is unsafe as it might be a buffer
	 * overflow
	 *
	 * This API is to check first whether GetAdress is safe to use.
	 */
	public boolean getAddressQuickTestOk(int offset, int length) {
		int chunkOffset = offset % chunkSize;

		return (length <= (chunkSize - chunkOffset));
	}

	public ByteBuffer getAddressAsByteBuffer(int offset) {
		int chunkNumber = offset / chunkSize;
		int chunkOffset = offset % chunkSize;

		ByteBuffer buffer = getChunkAsByteBuffer(chunkNumber);
		buffer.position(chunkOffset);

		return buffer;
	}

	public ByteBuffer getBuffer(int offset, int length) {
		int chunkNumber = offset / chunkSize;
		int chunkOffset = offset % chunkSize;

		int firstChunkSize = Math.min(length, chunkSize - chunkOffset);
		int secondChunkSize = length - firstChunkSize;

		ByteBuffer buffer = getChunkAsByteBuffer(chunkNumber);
		buffer.position(chunkOffset);

		ByteBuffer buffer2 = getChunkAsByteBuffer(chunkNumber + 1);

		ByteBuffer b = ByteBuffer.allocate(length);
		b.order(ByteOrder.LITTLE_ENDIAN);
		b.put(buffer);
		for (int i = 0; i < secondChunkSize; ++i) {
			b.put(buffer2.get(i));
		}
		b.position(0);
		return b;
	}

	public void append(byte[] buffer, int bufferLength) {
		int remaining = bufferLength;
		int bufferOffset = 0;

		while (remaining > 0) {
			int chunkNumber = tail / chunkSize;
			int chunkOffset = tail % chunkSize;

			ByteBuffer chunk = getChunkAsByteBuffer(chunkNumber);
			chunk.position(chunkOffset);

			int copySize = Math.min(remaining, chunkSize - chunkOffset);
			chunk.put(buffer, bufferOffset, copySize);

			remaining -= copySize;
			bufferOffset += copySize;
			tail += copySize;
		}
	}

	public void append(short[] buffer, int bufferLength) {
	  int remaining = bufferLength * 2;
	  int bufferOffset = 0;
	  while (remaining > 0) {
	    int chunkNumber = tail / chunkSize;
      int chunkOffset = tail % chunkSize;
      
      ByteBuffer chunk = getChunkAsByteBuffer(chunkNumber);
      chunk.position(chunkOffset);
      int copySize = Math.min(remaining, chunkSize - chunkOffset);
      ShortBuffer chunkShort  = chunk.asShortBuffer();
      chunkShort.put(buffer, bufferOffset/2, copySize / 2);
      
      remaining -= copySize;
      bufferOffset += copySize;
      tail += copySize;
	  }
	}

	public void append(byte c) {
		int chunkNumber = tail / chunkSize;
		int chunkOffset = tail % chunkSize;

		ByteBuffer chunk = getChunkAsByteBuffer(chunkNumber);
		chunk.position(chunkOffset);

		chunk.put(c);
		++tail;
	}

	public boolean equals(int offset, char[] buffer) {
		// TODO: implement if needed?? (needed vor value store persistence)

		return false;
	}
	

  public void write(DataOutput out, int end) throws IOException {
    if (mappings.size() == 0) {
      return;
    } else {
      int remaining = end;
      int chunk = 0;

      // we need to copy it twice, unfortunately, to be optimized
      byte[] byteArray = new byte[chunkSize];
      
      while (remaining > 0) {
        int bytesInChunk = Math.min(chunkSize, remaining);
        
        ByteBuffer buffer = mappings.get(chunk);
        buffer.position(0);
        buffer.get(byteArray, 0, bytesInChunk);
        out.writeBytes(byteArray, bytesInChunk);
        
        remaining -= bytesInChunk;
        ++chunk;
      }
    }
    
  }

	public void write(OutputStream stream, int end) throws IOException {
		if (mappings.size() == 0) {
			return;
		} else {
			int remaining = end;
			int chunk = 0;

			while (remaining > 0) {
				int bytesInChunk = Math.min(chunkSize, remaining);
				WritableByteChannel channel = Channels.newChannel(stream);

				ByteBuffer buffer = mappings.get(chunk);
				buffer.position(0);
				if (bytesInChunk < chunkSize) {
					buffer.limit(bytesInChunk);
				}
				
				channel.write(buffer);
				
				remaining -= bytesInChunk;
				++chunk;
			}
		}
	}

	public int size() {
		return tail;
	}

	public void persist() {
		// TODO: implement as needed
	}

	/*
	 * Get a buffer as copy.
	 *
	 * This API is to be used when GetAdress is not safe to use.
	 */
	/*
	 * void GetBuffer(const size_t offset, void* buffer, const size_t buffer_length)
	 * { size_t chunk_number = offset / chunk_size_; size_t chunk_offset = offset %
	 * chunk_size_;
	 * 
	 * void* chunk_address = GetChunk(chunk_number); void* chunk_address_part2 =
	 * GetChunk(chunk_number + 1);
	 * 
	 * size_t first_chunk_size = std::min(buffer_length, chunk_size_ -
	 * chunk_offset); size_t second_chunk_size = buffer_length - first_chunk_size;
	 * 
	 * std::memcpy(buffer, (char*) chunk_address + chunk_offset, first_chunk_size);
	 * std::memcpy((char*) buffer + first_chunk_size, (char*) chunk_address_part2,
	 * second_chunk_size); }
	 */

	private ByteBuffer getChunkAsByteBuffer(int chunkNumber) {
		while (chunkNumber >= mappings.size()) {
			try {
				createMapping();
			} catch (IOException e) {
				throw new RuntimeException("unexpected error");
			}
		}

		return mappings.get(chunkNumber);
	}

	private void createMapping() throws FileNotFoundException, IOException {
		RandomAccessFile newFile = new RandomAccessFile(File.createTempFile(filenamePattern, null, directory.toFile()),
				"rw");
		files.add(newFile);

		MappedByteBuffer newMapping = newFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, chunkSize);

		newMapping.order(ByteOrder.LITTLE_ENDIAN);
		mappings.add(newMapping);
	}

  public int getChunkSize() {
    return chunkSize;
  }
}
